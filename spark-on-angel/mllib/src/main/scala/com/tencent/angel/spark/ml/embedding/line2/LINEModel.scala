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


package com.tencent.angel.spark.ml.embedding.line2

import java.text.SimpleDateFormat
import java.util.Date

import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.model.{MatrixSaveContext, ModelSaveContext}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.embedding.NEModel.NEDataSet
import com.tencent.angel.spark.ml.embedding.line.LINEModel.{LINEDataSet, buildDataBatches}
import com.tencent.angel.spark.ml.embedding.{FastSigmoid, Param}
import com.tencent.angel.spark.ml.psf.embedding.NEModelRandomize.RandomizeUpdateParam
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.ints.{Int2IntOpenHashMap, Int2ObjectOpenHashMap}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Random

class LINEModel(numNode: Int,
                dimension: Int,
                numPart: Int,
                numNodesPerRow: Int = -1,
                order: Int = 1,
                seed: Int = Random.nextInt) extends Serializable {

  val matrixName = "embedding"
  // Create one ps matrix to hold the input vectors and the output vectors for all node
  val mc: MatrixContext = new MatrixContext(matrixName, 1, numNode)
  mc.setMaxRowNumInBlock(1)
  mc.setMaxColNumInBlock(numNode / numPart)
  mc.setRowType(RowType.T_ANY_INTKEY_DENSE)
  mc.setValueType(classOf[LINENode])

  val psMatrix: PSMatrix = PSMatrix.matrix(mc)
  val matrixId: Int = psMatrix.id
  randomInitialize(seed)

  // initialize embeddings
  def randomInitialize(seed: Int) = {
    val beforeRandomize = System.currentTimeMillis()
    psMatrix.psfUpdate(new LINEModelRandomize(new RandomizeUpdateParam(matrixId, dimension / numPart, dimension, order, seed))).get()
    logTime(s"Model successfully Randomized, cost ${(System.currentTimeMillis() - beforeRandomize) / 1000.0}s")
  }

  private val rand = new Random(seed)

  def this(param: Param) {
    this(param.maxIndex, param.embeddingDim, param.numPSPart, param.nodesNumPerRow, param.order, param.seed)
  }

  def train(trainSet: RDD[(Int, Int)], params: Param, path: String): this.type = {
    // Get mini-batch data set
    val trainBatches = buildDataBatches(trainSet, params.batchSize)

    val numEpoch = params.numEpoch
    val learningRate = params.learningRate
    val checkpointInterval = params.checkpointInterval
    val negative = params.negSample

    for (epoch <- 1 to numEpoch) {
      val alpha = learningRate
      val data = trainBatches.next()
      val numPartitions = data.getNumPartitions
      val middle = data.mapPartitionsWithIndex((partitionId, iterator) =>
        sgdForPartition(partitionId, iterator, numPartitions, negative, alpha),
        preservesPartitioning = true
      ).collect()
      val loss = middle.map(f => f._1).sum / middle.map(_._2).sum.toFloat
      val array = new Array[Long](6)
      middle.foreach(f => f._3.zipWithIndex.foreach(t => array(t._2) += t._1))

      logTime(s"epoch=$epoch " +
        f"loss=$loss%2.4f " +
        s"sampleTime=${array(0)} getEmbeddingTime=${array(1)} " +
        s"dotTime=${array(2)} gradientTime=${array(3)} calUpdateTime=${array(4)} pushTime=${array(5)} " +
        s"total=${middle.map(_._2).sum.toFloat} lossSum=${middle.map(_._1).sum} ")

      if (epoch % checkpointInterval == 0)
        save(path, epoch)
    }

    this
  }

  def sgdForPartition(partitionId: Int,
                      iterator: Iterator[NEDataSet],
                      numPartitions: Int,
                      negative: Int,
                      alpha: Float): Iterator[(Float, Long, Array[Long])] = {

    PSContext.instance()
    iterator.zipWithIndex.map { case (batch, index) =>
      sgdForBatch(partitionId, batch.asInstanceOf[LINEDataSet], negative, index, alpha, rand.nextInt())
    }
  }

  def sgdForBatch(partitionId: Int,
                  batch: LINEDataSet,
                  negative: Int,
                  batchId: Int,
                  alpha: Float, seed: Int): (Float, Long, Array[Long]) = {
    var start = 0L

    start = System.currentTimeMillis()
    val srcNodes = batch.src
    val destNodes = batch.dst
    val negativeSamples = negativeSample(srcNodes, negative, seed)
    val sampleTime = System.currentTimeMillis() - start

    // Get node embedding from PS
    start = System.currentTimeMillis()
    val getResult = getEmbedding(srcNodes, destNodes, negativeSamples, negative)
    val srcFeats:Int2ObjectOpenHashMap[Array[Float]] = getResult._1
    val targetFeats:Int2ObjectOpenHashMap[Array[Float]] = getResult._2

    val getEmbeddingTime = System.currentTimeMillis() - start

    // Get dot values
    start = System.currentTimeMillis()
    val dots = dot(srcNodes, destNodes, negativeSamples, srcFeats, targetFeats, negative)
    val dotTime = System.currentTimeMillis() - start

    // gradient
    start = System.currentTimeMillis()
    val loss = doGrad(dots, negative, alpha)
    val gradientTime = System.currentTimeMillis() - start

    start = System.currentTimeMillis()
    val (inputUpdates, outputUpdates) = adjust(srcNodes, destNodes, negativeSamples, srcFeats, targetFeats, negative, dots)
    val calUpdateTime = System.currentTimeMillis() - start

    start = System.currentTimeMillis()
    psMatrix.psfUpdate(new LINEAdjust(new LINEAdjustParam(matrixId, inputUpdates, outputUpdates, order)))
    val pushTime = System.currentTimeMillis() - start

    //if(batchId % 100 == 0) {
    logTime(s"loss=$loss sampleTime=${sampleTime} getEmbeddingTime=${getEmbeddingTime} " +
        s"dotTime=${dotTime} gradientTime=${gradientTime} calUpdateTime=${calUpdateTime} " +
        s"pushTime=${pushTime}")
    //}

    (loss, dots.length.toLong, Array(sampleTime, getEmbeddingTime, dotTime, gradientTime, calUpdateTime, pushTime))
  }

  def getEmbedding(srcNodes: Array[Int], destNodes: Array[Int], negativeSamples: Array[Array[Int]], negative: Int) = {
    psMatrix.psfGet(new LINEGetEmbedding(new LINEGetEmbeddingParam(matrixId, srcNodes, destNodes,
      negativeSamples, order, negative))).asInstanceOf[LINEGetEmbeddingResult].getResult
  }

  def dot(srcNodes: Array[Int], destNodes: Array[Int], negativeSamples: Array[Array[Int]],
          srcFeats: Int2ObjectOpenHashMap[Array[Float]], targetFeats: Int2ObjectOpenHashMap[Array[Float]], negative: Int):Array[Float] = {
    val dots: Array[Float] = new Array[Float]((1 + negative) * srcNodes.length)
    if (order == 1) {
      var docIndex = 0
      for (i <- 0 until srcNodes.length) {
        val srcVec = srcFeats.get(srcNodes(i))
        // Get dot value for (src, dst)
        dots(docIndex) = arraysDot(srcVec, srcFeats.get(destNodes(i)))
        docIndex += 1

        // Get dot value for (src, negative sample)
        for (j <- 0 until negative) {
          dots(docIndex) = arraysDot(srcVec, srcFeats.get(negativeSamples(i)(j)))
          docIndex += 1
        }
      }
      dots
    } else {
      var docIndex = 0
      for (i <- 0 until srcNodes.length) {
        val srcVec = srcFeats.get(srcNodes(i))
        // Get dot value for (src, dst)
        dots(docIndex) = arraysDot(srcVec, targetFeats.get(destNodes(i)))
        docIndex += 1

        // Get dot value for (src, negative sample)
        for (j <- 0 until negative) {
          dots(docIndex) = arraysDot(srcVec, targetFeats.get(negativeSamples(i)(j)))
          docIndex += 1
        }
      }
      dots
    }
  }

  def arraysDot(x: Array[Float], y: Array[Float]): Float = {
    var dotValue = 0.0f
    (0 until x.length).foreach(i => dotValue += x(i) * y(i))
    dotValue
  }

  def axpy(y: Array[Float], x: Array[Float], a: Float) = {
    (0 until x.length).foreach(i => y(i) += a * x(i))
  }

  def div(x: Array[Float], f: Float): Unit = {
    (0 until x.length).foreach(i => x(i) = x(i) / f)
  }

  def adjust(srcNodes: Array[Int], destNodes: Array[Int], negativeSamples: Array[Array[Int]],
             srcFeats: Int2ObjectOpenHashMap[Array[Float]], targetFeats: Int2ObjectOpenHashMap[Array[Float]],
             negative: Int, dots: Array[Float]) = {
    if (order == 1) {
      val inputUpdateCounter = new Int2IntOpenHashMap(srcFeats.size())
      val inputUpdates = new Int2ObjectOpenHashMap[Array[Float]](srcFeats.size())

      var docIndex = 0
      for (i <- 0 until srcNodes.length) {
        // Src node grad
        val neule = new Array[Float](dimension)

        // Accumulate dst node embedding to neule
        val dstEmbedding = srcFeats.get(destNodes(i))
        var g = dots(docIndex)
        axpy(neule, dstEmbedding, g)

        // Use src node embedding to update dst node embedding
        val srcEmbedding = srcFeats.get(srcNodes(i))
        merge(inputUpdateCounter, inputUpdates, destNodes(i), g, srcEmbedding)

        docIndex += 1

        // Use src node embedding to update negative sample node embedding; Accumulate negative sample node embedding to neule
        for (j <- 0 until negative) {
          val negSampleEmbedding = srcFeats.get(negativeSamples(i)(j))
          g = dots(docIndex)

          // Accumulate negative sample node embedding to neule
          axpy(neule, negSampleEmbedding, g)

          // Use src node embedding to update negative sample node embedding
          merge(inputUpdateCounter, inputUpdates, negativeSamples(i)(j), g, srcEmbedding)
          docIndex += 1
        }

        // Use accumulation to update src node embedding, grad = 1
        merge(inputUpdateCounter, inputUpdates, srcNodes(i), 1, neule)
      }

      val iter = inputUpdateCounter.entrySet().iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        div(inputUpdates.get(entry.getKey.toInt), entry.getValue.toFloat)
      }

      (inputUpdates, null)
    } else {
      val inputUpdateCounter = new Int2IntOpenHashMap(srcFeats.size())
      val inputUpdates = new Int2ObjectOpenHashMap[Array[Float]](srcFeats.size())

      val outputUpdateCounter = new Int2IntOpenHashMap(targetFeats.size())
      val outputUpdates = new Int2ObjectOpenHashMap[Array[Float]](targetFeats.size())

      var docIndex = 0
      for (i <- 0 until srcNodes.length) {
        // Src node grad
        val neule = new Array[Float](dimension)

        // Accumulate dst node embedding to neule
        val dstEmbedding = targetFeats.get(destNodes(i))
        var g = dots(docIndex)
        axpy(neule, dstEmbedding, g)

        // Use src node embedding to update dst node embedding
        val srcEmbedding = srcFeats.get(srcNodes(i))
        merge(outputUpdateCounter, outputUpdates, destNodes(i), g, srcEmbedding)

        docIndex += 1

        // Use src node embedding to update negative sample node embedding; Accumulate negative sample node embedding to neule
        for (j <- 0 until negative) {
          val negSampleEmbedding = targetFeats.get(negativeSamples(i)(j))
          g = dots(docIndex)

          // Accumulate negative sample node embedding to neule
          axpy(neule, negSampleEmbedding, g)

          // Use src node embedding to update negative sample node embedding
          merge(outputUpdateCounter, outputUpdates, negativeSamples(i)(j), g, srcEmbedding)
          docIndex += 1
        }

        // Use accumulation to update src node embedding, grad = 1
        merge(inputUpdateCounter, inputUpdates, srcNodes(i), 1, neule)
      }

      var iter = inputUpdateCounter.int2IntEntrySet().fastIterator()
      while (iter.hasNext) {
        val entry = iter.next()
        div(inputUpdates.get(entry.getIntKey), entry.getIntValue.toFloat)
      }

      iter = outputUpdateCounter.int2IntEntrySet().fastIterator()
      while (iter.hasNext) {
        val entry = iter.next()
        div(outputUpdates.get(entry.getIntKey), entry.getIntValue.toFloat)
      }

      (inputUpdates, outputUpdates)
    }
  }

  def merge(inputUpdateCounter: Int2IntOpenHashMap, inputUpdates: Int2ObjectOpenHashMap[Array[Float]],
            nodeId: Int, g: Float, update: Array[Float]) = {
    var grads: Array[Float] = inputUpdates.get(nodeId)
    if (grads == null) {
      grads = new Array[Float](dimension)
      inputUpdates.put(nodeId, grads)
      inputUpdateCounter.put(nodeId, 0)
    }

    //grads.iaxpy(update, g)
    axpy(grads, update, g)
    inputUpdateCounter.addTo(nodeId, 1)
  }


  def save(modelPathRoot: String, epoch: Int): Unit = {
    val modelPath = new Path(modelPathRoot, s"CP_$epoch").toString
    val startTime = System.currentTimeMillis()
    logTime(s"saving model to $modelPath")
    val ss = SparkSession.builder().getOrCreate()
    deleteIfExists(modelPath, ss)

    val saveContext = new ModelSaveContext(modelPath)
    saveContext.addMatrix(new MatrixSaveContext(matrixName, classOf[TextLINEModelOutputFormat].getTypeName))
    PSContext.getOrCreate(SparkContext.getOrCreate()).save(saveContext)
    logTime(s"model save time=${System.currentTimeMillis() - startTime} ms")
  }

  def negativeSample(nodeIds: Array[Int], sampleNum: Int, seed: Int) = {
    //val seed = UUID.randomUUID().hashCode()
    val rand = new Random(seed)
    val sampleNodes = new Array[Array[Int]](nodeIds.length)
    var nodeIndex: Int = 0

    for (nodeId <- nodeIds) {
      var sampleIndex: Int = 0
      sampleNodes(nodeIndex) = new Array[Int](sampleNum)
      while (sampleIndex < sampleNum) {
        val target = rand.nextInt(numNode)
        if (target != nodeId) {
          sampleNodes(nodeIndex)(sampleIndex) = target
          sampleIndex += 1
        }
      }
      nodeIndex += 1
    }
    sampleNodes
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

  def logTime(msg: String): Unit = {
    val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    println(s"[$time] $msg")
  }

  def doGrad(dots: Array[Float], negative: Int, alpha: Float): Float = {
    var loss = 0.0f
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
