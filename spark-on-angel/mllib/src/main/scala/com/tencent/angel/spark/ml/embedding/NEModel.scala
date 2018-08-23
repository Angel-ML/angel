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
import com.tencent.angel.spark.ml.embedding.NEModel._
import com.tencent.angel.spark.ml.psf.embedding.NEDot.NEDotResult
import com.tencent.angel.spark.ml.psf.embedding.NESlice.SliceResult
import com.tencent.angel.spark.ml.psf.embedding.{NEModelRandomize, NENSTableInitializer, NESlice}
import com.tencent.angel.spark.models.matrix.{DensePSMatrix, PSMatrix}

abstract class NEModel(numNode: Int,
                       dimension: Int,
                       numPart: Int,
                       numNodesPerRow: Int = -1,
                       order: Int = 2,
                       var seed: Int = Random.nextInt())
  extends Serializable {
  val partDim: Int = dimension / numPart
  require(dimension % numPart == 0, "dimension must be times of numPart, (dimension % numPart == 0)")
  val psMatrix: DensePSMatrix = createPSMatrix(partDim * order, numNode, numPart, numNodesPerRow)
  val matrixId: Int = psMatrix.id
  private val rand = new Random(seed)
  // initialize embeddings
  randomInitialize(rand.nextInt)
  createNegativeSampleTable(numNode, rand.nextInt, 0)

  def getDotPsf(data: NEDataSet, batchSeed: Int, ns: Int, window: Option[Int]): GetFunc

  def getAdjustPsf(data: NEDataSet, batchSeed: Int, ns: Int, grad: Array[Float], window: Option[Int]): UpdateFunc

  def train(trainBatchesRDDIter: Iterator[RDD[NEDataSet]],
            validBatchesOpt: Option[RDD[NEDataSet]],
            ns: Int,
            window: Option[Int],
            numEpoch: Int,
            lr: Float,
            modelPath: String,
            modelCPInterval: Int,
            logEveryBatchNum: Option[Double] = None): this.type = {
    val r = new Random(rand.nextInt)
    for (i <- 1 to numEpoch) {
      val alpha = lr
      val trainDataSet = trainBatchesRDDIter.next()
      val trainMeanLoss = calcTrainingLoss {
        trainDataSet.mapPartitions { batchIter =>
          var batchCounter = 0
          var percent = 1
          batchIter.map { batch =>
            logEveryBatchNum.foreach { logStep =>
              if (batchCounter / logStep >= percent) {
                logTime(s"epoch: $i, finished $percent%")
                percent += 1
              }
              batchCounter += 1
            }
            miniBatchGD(batch, ns, r.nextInt(), alpha, window)
          }
        }
      }
      logTime(s"epoch: $i, train mean loss = $trainMeanLoss")
      validBatchesOpt.foreach { validSet =>
        val validMeanLoss = calcTrainingLoss {
          validSet.map(batchEvaluation(_, r.nextInt(), ns, window))
        }
        logTime(s"epoch: $i, validate mean loss = $validMeanLoss")
      }
      // save
      if (i % modelCPInterval == 0 || i == numEpoch) save(modelPath, i)
    }
    this
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

  private def psfGet(func: GetFunc): GetResult = {
    psMatrix.psfGet(func)
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

  private def calcTrainingLoss(loss: RDD[(Double, Long)]): Double = {
    val (lossSum, dataSize) = loss.reduce { case ((lossSum1, batchSize1), (lossSum2, batchSize2)) =>
      (lossSum1 + lossSum2, batchSize1 + batchSize2)
    }
    lossSum / dataSize
  }

  private def miniBatchGD(data: NEDataSet, ns: Int, seed: Int, alpha: Float, window: Option[Int]): (Double, Long) = {
    val batchSeed = new Random(seed).nextInt()
    if (Random.nextDouble() > 0.001) {
      val dotRet = psfGet(getDotPsf(data, batchSeed, ns, window)).asInstanceOf[NEDotResult].result
      val loss = NEModel.doGrad(dotRet, ns, alpha)
      pdfUpdate(getAdjustPsf(data, batchSeed, ns, dotRet, window))
      (loss, dotRet.length.toLong)
    } else {
      // print some information for performance diagnosis
      val beforeDot = System.currentTimeMillis()
      val dotRet = psfGet(getDotPsf(data, batchSeed, ns, window)).asInstanceOf[NEDotResult].result
      val beforeGrad = System.currentTimeMillis()
      val loss = NEModel.doGrad(dotRet, ns, alpha)
      val beforeAdjust = System.currentTimeMillis()
      pdfUpdate(getAdjustPsf(data, batchSeed, ns, dotRet, window))
      val finished = System.currentTimeMillis()
      logTime(s"(dot, grad, adjust): (${beforeGrad - beforeDot}, ${beforeAdjust - beforeGrad}, ${finished - beforeAdjust})")
      (loss, dotRet.length.toLong)
    }
  }

  def batchEvaluation(data: NEDataSet, seed: Int, ns: Int, window: Option[Int]): (Double, Long) = {
    psfGet(getDotPsf(data: NEDataSet, seed, ns, None)).asInstanceOf[NEDotResult].result
      .foldLeft[(Double, Long)]((0.0, 0L)) { case ((s, i), v) =>
      val prob = FastSigmoid.sigmoid(v)
      val lo = if (i % (ns + 1) == 0) -FastSigmoid.log(prob) else -FastSigmoid.log(1 - prob)
      (lo + s, i + 1)
    }
  }

  private def randomInitialize(seed: Int): Unit = {
    val beforeRandomize = System.currentTimeMillis()
    pdfUpdate(new NEModelRandomize(matrixId, dimension / numPart * order, dimension, order, seed))
    logTime(s"Model successfully Randomized", beforeRandomize)
  }

  private def pdfUpdate(func: UpdateFunc): VoidResult = {
    psMatrix.psfUpdate(func).get
  }

  private def createNegativeSampleTable(maxIndex: Int, seed: Int, version: Int) {
    val beforeTableInitialized = System.currentTimeMillis()
    pdfUpdate(new NENSTableInitializer(matrixId, maxIndex, seed, version))
    logTime(s"NegativeSampleTableInitialized", beforeTableInitialized)
  }

  private def createPSMatrix(sizeOccupiedPerNode: Int,
                             numNode: Int,
                             numPart: Int,
                             numNodesPerRow: Int = -1): DensePSMatrix = {
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
}

object NEModel {
  private def doGrad(dotRet: Array[Float], ns: Int, alpha: Float): Double = {
    var loss = 0.0
    for (i <- dotRet.indices) {
      val prob = FastSigmoid.sigmoid(dotRet(i))
      if (i % (ns + 1) == 0) {
        dotRet(i) = alpha * (1 - prob)
        loss -= FastSigmoid.log(prob)
      } else {
        dotRet(i) = -alpha * FastSigmoid.sigmoid(dotRet(i))
        loss -= FastSigmoid.log(1 - prob)
      }
    }
    loss
  }

  private def logTime(msg: String, begin: Long = -1): Long = {
    val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    val end = System.currentTimeMillis()
    println(if (begin > 0) s"[$time] $msg, cost ${(end - begin) / 1000.0}s" else s"[$time] $msg")
    end
  }

  abstract class NEDataSet

}
