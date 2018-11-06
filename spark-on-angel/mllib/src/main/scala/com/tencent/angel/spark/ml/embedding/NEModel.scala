/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.spark.ml.embedding

import java.text.SimpleDateFormat
import java.util.Date

import scala.util.Random
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, GetResult}
import com.tencent.angel.ml.matrix.psf.update.base.{UpdateFunc, VoidResult}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.embedding.NEModel._
import com.tencent.angel.spark.ml.psf.embedding.NEDot.NEDotResult
import com.tencent.angel.spark.ml.psf.embedding.NESlice.SliceResult
import com.tencent.angel.spark.ml.psf.embedding.{NEModelRandomize, NENSTableInitializer, NESlice}
import com.tencent.angel.spark.models.PSMatrix

abstract class NEModel(numNode: Int,
                       dimension: Int,
                       numPart: Int,
                       numNodesPerRow: Int = -1,
                       order: Int = 2,
                       useNegativeTable: Boolean = true,
                       var seed: Int = Random.nextInt()) extends Serializable {

  // partDim is the dimensions for one node in each server partition
  val partDim: Int = dimension / numPart
  require(dimension % numPart == 0, "dimension must be times of numPart, (dimension % numPart == 0)")

  // Create one ps matrix to hold the input vectors and the output vectors for all node
  val psMatrix: PSMatrix = createPSMatrix(partDim * order, numNode, numPart, numNodesPerRow)
  val matrixId: Int = psMatrix.id

  private val rand = new Random(seed)

  // initialize embeddings
  randomInitialize(rand.nextInt)

  // create the negative sampling table on each server
  if (useNegativeTable)
    createNegativeSampleTable(numNode, rand.nextInt, 0)

  def getDotFunc(data: NEDataSet, batchSeed: Int, ns: Int, window: Option[Int],
                 partitionId: Option[Int] = None): GetFunc

  def getAdjustFunc(data: NEDataSet, batchSeed: Int, ns: Int, grad: Array[Float], window: Option[Int],
                    partitionId: Option[Int] = None): UpdateFunc

  def getInitFunc(numPartitions: Int, maxIndex: Int, maxLength: Option[Int]): Option[UpdateFunc]

  /**
    * Main function for training embedding model
    * @param trainBatches
    * @param validate
    * @param negative
    * @param window
    * @param numEpoch
    * @param learningRate
    * @param checkpointInterval
    * @return
    */
  def train(trainBatches: Iterator[RDD[NEDataSet]],
            validate: Option[RDD[NEDataSet]],
            negative: Int,
            window: Option[Int],
            numEpoch: Int,
            learningRate: Float,
            maxLength: Option[Int],
            checkpointInterval: Int): Unit = {
    for (epoch <- 1 to numEpoch) {
      val alpha = learningRate
      val data = trainBatches.next()
      val numPartitions = data.getNumPartitions
      val middle = data.mapPartitionsWithIndex(
        (partitionId, iterator) => sgdForPartition(partitionId, iterator,
          numPartitions, negative, window, maxLength, alpha, epoch),
        true).collect()
      val loss = middle.map(f => f._1).sum
      val array = new Array[Long](3)
      middle.foreach(f => f._2.zipWithIndex.foreach(t => array(t._2) += t._1))
      logTime(s"epoch=$epoch " +
        s"loss=$loss " +
        s"dotTime=${array(0)} " +
        s"gradientTime=${array(1)} " +
        s"adjustTime=${array(2)}")
    }
  }

  def save(modelPathRoot: String, epoch: Int): Unit = {
    val modelPath = new Path(modelPathRoot, s"CP_$epoch").toString
    val startTime = logTime(s"saving model to $modelPath")
    val ss = SparkSession.builder().getOrCreate()
    deleteIfExists(modelPath, ss)
    slicedSavingRDDBuilder(ss, partDim)
      .flatMap[String](getSlice)
      .saveAsTextFile(modelPath)
    logTime("saving finished", startTime)
  }

  private def getSlice(slicePair: (Int, Int)): Array[String] = {
    val (from, until) = slicePair
    logTime(s"get nodes with id ranging from $from until $until")
    psfGet(new NESlice(psMatrix.id, from, until - from, partDim, order)).asInstanceOf[SliceResult].getSlice()
  }

  private def slicedSavingRDDBuilder(ss: SparkSession, partDim: Int): RDD[(Int, Int)] = {
    val numExecutor = getAvailableExecutorNum(ss)
    val numNodePerPartition = math.max(1, math.min(100000, (numNode - 1) / numExecutor + 1))
    val sliceBatchSize = math.min(1000000 / partDim, numNodePerPartition)
    val slices = (Array.range(0, numNode, sliceBatchSize) ++ Array(numNode))
      .sliding(2).map(arr => (arr(0), arr(1))).toArray
    val partitionNum = math.min((numNode - 1) / numNodePerPartition + 1, slices.length)
    logTime(s"make slices: ${slices.take(5).mkString(" ")}, partitionNum: $partitionNum")
    ss.sparkContext.makeRDD(slices, partitionNum)
  }

  private def getAvailableExecutorNum(ss: SparkSession): Int = {
    math.max(ss.sparkContext.statusTracker.getExecutorInfos.length - 1, 1)
  }

  private def deleteIfExists(modelPath: String, ss: SparkSession): Unit = {
    val path = new Path(modelPath)
    val fs = path.getFileSystem(ss.sparkContext.hadoopConfiguration)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
  }

  def sgdForPartition(partitionId: Int,
                      iterator: Iterator[NEDataSet],
                      numPartitions: Int,
                      negative: Int,
                      window: Option[Int],
                      maxLength: Option[Int],
                      alpha: Float,
                      epoch: Int): Iterator[(Double, Array[Long])] = {

    def sgdForBatch(partitionId: Int,
                    seed: Int,
                    batch: NEDataSet,
                    batchId: Int): (Double, Array[Long]) = {
      var (start, end) = (0L, 0L)
      // dot
      start = System.currentTimeMillis()
      val dots = psfGet(getDotFunc(batch, seed, negative, window, Some(partitionId)))
        .asInstanceOf[NEDotResult].result
      end = System.currentTimeMillis()
      val dotTime = end - start
      // gradient
      start = System.currentTimeMillis()
      val loss = doGrad(dots, negative, alpha, Some(batch))
      end = System.currentTimeMillis()
      val gradientTime = end - start
      // adjust
      start = System.currentTimeMillis()
      psfUpdate(getAdjustFunc(batch, seed, negative, dots, window, Some(partitionId)))
      end = System.currentTimeMillis()
      val adjustTime = end - start
      // return loss
      if ((batchId + 1) % 100 == 0)
        println(s"batchId=$batchId dotTime=$dotTime gradientTime=$gradientTime adjustTime=$adjustTime")
      (loss, Array(dotTime, gradientTime, adjustTime))
    }

    PSContext.instance()

    if (epoch == 1)
      getInitFunc(numPartitions, numNode, maxLength).map(func => psfUpdate(func))

    iterator.zipWithIndex.map { case (batch, index) =>
      sgdForBatch(partitionId, rand.nextInt(), batch, index)
    }
  }

  def batchEvaluation(data: NEDataSet, seed: Int, ns: Int, window: Option[Int]): (Double, Long) = {
    psfGet(getDotFunc(data: NEDataSet, seed, ns, None)).asInstanceOf[NEDotResult].result
      .foldLeft[(Double, Long)]((0.0, 0L)) { case ((s, i), v) =>
      val prob = FastSigmoid.sigmoid(v)
      val lo = if (i % (ns + 1) == 0) -FastSigmoid.log(prob) else -FastSigmoid.log(1 - prob)
      (lo + s, i + 1)
    }
  }

  private def randomInitialize(seed: Int): Unit = {
    val beforeRandomize = System.currentTimeMillis()
    psfUpdate(new NEModelRandomize(matrixId, dimension / numPart * order, dimension, order, seed))
    logTime(s"Model successfully Randomized", beforeRandomize)
  }

  private def psfUpdate(func: UpdateFunc): VoidResult = {
    psMatrix.psfUpdate(func).get
  }

  private def psfGet(func: GetFunc): GetResult = {
    psMatrix.psfGet(func)
  }

  private def createNegativeSampleTable(maxIndex: Int, seed: Int, version: Int) {
    val beforeTableInitialized = System.currentTimeMillis()
    psfUpdate(new NENSTableInitializer(matrixId, maxIndex, seed, version))
    logTime(s"NegativeSampleTableInitialized", beforeTableInitialized)
  }

  private def createPSMatrix(sizeOccupiedPerNode: Int,
                             numNode: Int,
                             numPart: Int,
                             numNodesPerRow: Int = -1): PSMatrix = {
    require(numNodesPerRow <= Int.MaxValue / sizeOccupiedPerNode,
      s"size exceed Int.MaxValue, $numNodesPerRow * $sizeOccupiedPerNode > ${Int.MaxValue}")

    val rowCapacity = if (numNodesPerRow > 0) numNodesPerRow else Int.MaxValue / sizeOccupiedPerNode
    val numRow = (numNode - 1) / rowCapacity + 1
    val numNodesEachRow = if (numNodesPerRow > 0) numNodesPerRow else (numNode - 1) / numRow + 1
    val numCol = numPart.toLong * numNodesEachRow * sizeOccupiedPerNode
    val rowsInBlock = numRow
    val colsInBlock = numNodesEachRow * sizeOccupiedPerNode
    logTime(s"matrix meta:\n" +
      s"colNum: $numCol\n" +
      s"rowNum: $numRow\n" +
      s"colsInBlock: $colsInBlock\n" +
      s"rowsInBlock: $rowsInBlock\n" +
      s"numNodesPerRow: $numNodesEachRow\n" +
      s"sizeOccupiedPerNode: $sizeOccupiedPerNode"
    )
    // create ps matrix
    val begin = System.currentTimeMillis()
    val psMatrix = PSMatrix.dense(numRow, numCol, rowsInBlock, colsInBlock, RowType.T_FLOAT_DENSE)
    logTime(s"Model created, takes ${(System.currentTimeMillis() - begin) / 1000.0}s")

    psMatrix
  }

  def doGrad(dots: Array[Float], negative: Int, alpha: Float, data: Option[NEDataSet]): Double = {
    var loss = 0.0
    for (i <- dots.indices) {
      val prob = FastSigmoid.sigmoid(dots(i))
      if (i % (negative + 1) == 0) {
        dots(i) = alpha * (1 - prob)
        loss -= FastSigmoid.log(prob)
      } else {
        dots(i) = -alpha * FastSigmoid.sigmoid(dots(i))
        loss -= FastSigmoid.log(1 - prob)
      }
    }
    loss
  }
}

object NEModel {

  def logTime(msg: String, begin: Long = -1): Long = {
    val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    val end = System.currentTimeMillis()
    println(if (begin > 0) s"[$time] $msg, cost ${(end - begin) / 1000.0}s" else s"[$time] $msg")
    end
  }

  abstract class NEDataSet

}
