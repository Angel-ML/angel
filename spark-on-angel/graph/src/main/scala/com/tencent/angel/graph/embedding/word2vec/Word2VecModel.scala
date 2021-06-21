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

package com.tencent.angel.graph.embedding.word2vec

import java.util.concurrent.TimeUnit

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.utils.ModelContextUtils
import com.tencent.angel.ml.core.optimizer.decayer.{StandardDecay, StepSizeScheduler}
import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.IntIntVector
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.model.output.format.ModelFilesConstent
import com.tencent.angel.model.{MatrixLoadContext, MatrixSaveContext, ModelLoadContext, ModelSaveContext}
import com.tencent.angel.spark.context.{AngelPSContext, PSContext}
import com.tencent.angel.graph.embedding.line._
import com.tencent.angel.graph.embedding.FastSigmoid
import com.tencent.angel.spark.ml.util.LogUtils
import com.tencent.angel.spark.models.{PSMatrix, PSVector}
import it.unimi.dsi.fastutil.ints.{Int2IntOpenHashMap, Int2ObjectOpenHashMap, IntArrayList}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class Word2VecModel(params: Word2VecParam) extends Serializable {

  val matrixName = "embedding"
  // Create one ps matrix to hold the input vectors and the output vectors for all node
  val modelContext = new ModelContext(params.numPSPart, params.minIndex, params.maxIndex,
    params.maxIndex - params.minIndex + 1, matrixName, SparkContext.getOrCreate().hadoopConfiguration)
  val mc: MatrixContext = ModelContextUtils.createMatrixContext(modelContext, RowType.T_ANY_INTKEY_SPARSE,
    classOf[LINENode])
  if (!modelContext.isUseHashPartition) {
    mc.setRowType(RowType.T_ANY_INTKEY_DENSE)
  }
  val psMatrix: PSMatrix = PSMatrix.matrix(mc)
  val matrixId: Int = psMatrix.id
  val ssScheduler: StepSizeScheduler = new StandardDecay(params.learningRate, params.decayRate)
  var totalPullTime: Long = 0
  var totalPushTime: Long = 0
  var totalMakeParamTime: Long = 0
  var totalCalTime: Long = 0
  var totalMakeGradTime: Long = 0
  var totalCallNum: Long = 0
  var totalWaitPullTime: Long = 0
  var nodeTypeMatrix: PSVector = null
  if (params.nodeTypePath.length > 0) {
    val nodeTypeModelContext = new ModelContext(params.numPSPart, params.minIndex, params.maxIndex,
      params.maxIndex - params.minIndex + 1, "nodeType", SparkContext.getOrCreate().hadoopConfiguration)
    val ctx: MatrixContext = ModelContextUtils.createMatrixContext(nodeTypeModelContext, RowType.T_INT_SPARSE)
    nodeTypeMatrix = PSVector.vector(ctx)
  }

  // initialize embeddings
  def randomInitialize(seed: Int): Unit = {
    val beforeRandomize = System.currentTimeMillis()
    if (modelContext.isUseHashPartition) {
      modelContext.getMinNodeId.toInt.to(modelContext.getMaxNodeId.toInt).sliding(1000000, 1000000).foreach(e => {
        psMatrix.asyncPsfUpdate(new LINEModelRandomizeAsNodes(
          new RandomizeUpdateAsNodesParam(matrixId, params.embeddingDim, e.toArray, 2, seed)))
          .get(120000, TimeUnit.MILLISECONDS)
      })
    } else {
      psMatrix.asyncPsfUpdate(new LINEModelRandomize(new RandomizeUpdateParam(matrixId, params.embeddingDim,
        2, seed))).get(120000, TimeUnit.MILLISECONDS)
    }
    LogUtils.logTime(s"Model successfully Randomized, cost ${(System.currentTimeMillis() - beforeRandomize) / 1000.0}s")
  }

  def extraInitialize(extraRDD: RDD[String], params: Word2VecParam): Unit = {
    randomInitialize(Random.nextInt())
    val conf = AngelPSContext.convertToHadoop(SparkContext.getOrCreate().getConf)
    val featSep = conf.get("angel.line.feature.sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
      case "colon" => ":"
      case "bar" => "\\|"
    }
    val keyValueSep = conf.get("angel.line.keyvalue.sep", "colon") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
      case "colon" => ":"
      case "bar" => "\\|"
    }
    val beforeInitialize = System.currentTimeMillis()
    extraRDD.mapPartitions { iterator =>
      iterator.sliding(5000, 5000)
        .map(batch => extraUpdate(batch.toArray, keyValueSep, featSep))
    }.count()
    LogUtils.logTime(s"Model successfully extra Initial, cost ${(System.currentTimeMillis() - beforeInitialize) / 1000.0}s")
  }

  def initNodeType(nodeTypeRDD: RDD[(Int, Int)], params: Word2VecParam): Long = {
    nodeTypeRDD.mapPartitions { iterator =>
      iterator.sliding(5000, 5000)
        .map{batch =>
          val (indices, values) = batch.toArray.sortBy(x => x._1).unzip
          val update = VFactory.sparseIntVector(params.maxIndex, indices, values)
          nodeTypeMatrix.update(update)
        }
    }.count()
  }

  def extraUpdate(strings: Array[String], keyValueSep: String, featSep: String): Unit = {
    val inputUpdates = new Int2ObjectOpenHashMap[Array[Float]]()
    strings.map{line =>
      if (featSep.equals(keyValueSep)) {
        val splits = line.split(keyValueSep)
        val key = splits(0).toInt
        val value = new Array[Float](splits.length - 1)
        (1 until splits.length).foreach(i => value(i - 1) = splits(i).toFloat)
        inputUpdates.put(key, value)
      } else {
        val splits = line.split(keyValueSep)
        val key = splits(0).toInt
        val value = splits(1).split(featSep).map(v => v.toFloat)
        inputUpdates.put(key, value)
      }
    }
    psMatrix.asyncPsfUpdate(new LINEAdjust(new LINEAdjustParam(matrixId, inputUpdates, null,
      2, true))).get(120000, TimeUnit.MILLISECONDS)
  }

  def train(corpus: RDD[Array[Int]], params: Word2VecParam): Unit = {
    var learningRate = params.learningRate
    val startTs = System.currentTimeMillis()
    // Before training, checkpoint the model
    psMatrix.checkpoint(0)
    LogUtils.logTime(s"Write checkpoint use time=${System.currentTimeMillis() - startTs}")
    for (epoch <- 1 to params.numEpoch) {
      val epochStartTime = System.currentTimeMillis()
      val (lossSum, size) = corpus.mapPartitions {
        iterator =>
          iterator.sliding(params.batchSize, params.batchSize)
            .map(batch => (optimize(batch.toArray, params.windowSize, params.negSample, params.maxIndex,
              params.minIndex, learningRate, params.logStep), 1))
      }.reduce((f1, f2) => (f1._1 + f2._1, f1._2 + f2._2))
      //learningRate = ssScheduler.next().toFloat
      val epochTime = System.currentTimeMillis() - epochStartTime
      println(s"epoch=$epoch lr=$learningRate loss=${lossSum / size} time=${epochTime.toFloat / 1000}s")
      learningRate = Math.max(params.learningRate*0.0001f, ssScheduler.next().toFloat)
      checkpointAndSaveIfNeed(epoch, params)
    }
  }

  /**
    * Write checkpoint or model result if need
    *
    * @param epoch
    */
  def checkpointAndSaveIfNeed(epoch: Int, params: Word2VecParam): Unit = {
    var startTs = 0L
    if (epoch % params.checkpointInterval == 0 && epoch < params.numEpoch) {
      LogUtils.logTime(s"Epoch=$epoch, checkpoint the model")
      startTs = System.currentTimeMillis()
      psMatrix.checkpoint(epoch)
      LogUtils.logTime(s"checkpoint use time=${System.currentTimeMillis() - startTs}")
    }

    if (epoch % params.saveModelInterval == 0 && epoch < params.numEpoch) {
      LogUtils.logTime(s"Epoch=$epoch, save the model")
      startTs = System.currentTimeMillis()
      save(params.modelPath, epoch,  params.saveMeta)
      LogUtils.logTime(s"save use time=${System.currentTimeMillis() - startTs}")
    }
  }

  def save(modelPathRoot: String, epoch: Int, saveMeta: Boolean): Unit = {
    save(new Path(modelPathRoot, s"CP_$epoch").toString, saveMeta)
  }

  def save(modelPath: String, saveMeta: Boolean): Unit = {
    LogUtils.logTime(s"saving model to $modelPath")
    val ss = SparkSession.builder().getOrCreate()
    deleteIfExists(modelPath, ss)

    val saveContext = new ModelSaveContext(modelPath)
    saveContext.addMatrix(new MatrixSaveContext(matrixName, classOf[TextLINEModelOutputFormat].getTypeName))
    PSContext.instance().save(saveContext)

    if(!saveMeta) {
      // Remove the meta file
      try {
        val metaPath = new Path(new Path(modelPath, psMatrix.name), ModelFilesConstent.modelMetaFileName)
        // Build hadoop conf
        val conf = AngelPSContext.convertToHadoop(SparkContext.getOrCreate().getConf)
        val fs = metaPath.getFileSystem(conf)
        // Remove
        val ret = fs.delete(metaPath, true)
        if(!ret) {
          LogUtils.logTime(s"Warning: remove meta file failed !!!")
        }
      } catch {
        case e:Throwable => LogUtils.logTime(s"Warning: remove meta file failed !!!")
      }
    }
  }

  def deleteIfExists(modelPath: String, ss: SparkSession): Unit = {
    val path = new Path(modelPath)
    val fs = path.getFileSystem(ss.sparkContext.hadoopConfiguration)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
  }

  def optimize(batch: Array[Array[Int]], windowSize: Int, numNegSample: Int, maxIndex: Int, minIndex: Int,
               learningRate: Float, logStep: Int): Double = {
    val trainBatch = Word2VecModel.parseBatchData(batch, windowSize, numNegSample, maxIndex, minIndex, nodeTypeMatrix)
    val loss = optimizeOneBatch(batch.length, trainBatch._1, trainBatch._2, trainBatch._3, numNegSample, learningRate, logStep)
    loss
  }

  def optimizeOneBatch(batchSize: Int, srcNodes: Array[Int], dstNodes: Array[Int], negativeSamples: Array[Array[Int]],
                       numNegSample: Int, learningRate: Float, logStep: Int): Double = {
    incCallNum()
    var start = 0L
    start = System.currentTimeMillis()
    val result = psMatrix.asyncPsfGet(new LINEGetEmbedding(new LINEGetEmbeddingParam(matrixId, srcNodes, dstNodes,
      negativeSamples, 2, numNegSample))).get(600000, TimeUnit.MILLISECONDS).asInstanceOf[LINEGetEmbeddingResult].getResult
    val srcFeats: Int2ObjectOpenHashMap[Array[Float]] = result._1
    val dstFeats: Int2ObjectOpenHashMap[Array[Float]] = result._2
    incPullTime(start)

    // Calculate the gradients
    start = System.currentTimeMillis()
    val dots = dot(srcNodes, dstNodes, negativeSamples, srcFeats, dstFeats, numNegSample)
    var loss = doGrad(dots, numNegSample, learningRate)
    incCalTime(start)
    start = System.currentTimeMillis()
    val (inputUpdates, outputUpdates) = adjust(srcNodes, dstNodes, negativeSamples, srcFeats, dstFeats, numNegSample, dots)
    incCalUpdateTime(start)
    // Push the gradient to ps
    start = System.currentTimeMillis()
    psMatrix.asyncPsfUpdate(new LINEAdjust(new LINEAdjustParam(matrixId, inputUpdates, outputUpdates, 2)))
      .get(600000, TimeUnit.MILLISECONDS)
    incPushTime(start)

    loss = loss / dots.length.toLong
    if (logFlag(logStep)) {
      println(s"srcNodesLen=${srcNodes.length} avgPullTime=$avgPullTime avgGradTime=$avgCalTime " +
        s"avgCalUpdateTime=$avgCalUpdateTime avgPushTime=$avgPushTime loss=$loss")
    }

    loss
  }

  def dot(srcNodes: Array[Int], destNodes: Array[Int], negativeSamples: Array[Array[Int]],
          srcFeats: Int2ObjectOpenHashMap[Array[Float]], targetFeats: Int2ObjectOpenHashMap[Array[Float]], negative: Int): Array[Float] = {
    val dots: Array[Float] = new Array[Float]((1 + negative) * srcNodes.length)
    var docIndex = 0
    for (i <- srcNodes.indices) {
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

  def arraysDot(x: Array[Float], y: Array[Float]): Float = {
    var dotValue = 0.0f
    x.indices.foreach(i => dotValue += x(i) * y(i))
    dotValue
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

  def adjust(srcNodes: Array[Int], destNodes: Array[Int], negativeSamples: Array[Array[Int]],
             srcFeats: Int2ObjectOpenHashMap[Array[Float]], targetFeats: Int2ObjectOpenHashMap[Array[Float]],
             negative: Int, dots: Array[Float]): (Int2ObjectOpenHashMap[Array[Float]], Int2ObjectOpenHashMap[Array[Float]]) = {
    val inputUpdateCounter = new Int2IntOpenHashMap(srcFeats.size())
    val inputUpdates = new Int2ObjectOpenHashMap[Array[Float]](srcFeats.size())

    val outputUpdateCounter = new Int2IntOpenHashMap(targetFeats.size())
    val outputUpdates = new Int2ObjectOpenHashMap[Array[Float]](targetFeats.size())

    var docIndex = 0
    for (i <- srcNodes.indices) {
      // Src node grad
      val neule = new Array[Float](params.embeddingDim)

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

  def merge(inputUpdateCounter: Int2IntOpenHashMap, inputUpdates: Int2ObjectOpenHashMap[Array[Float]],
            nodeId: Int, g: Float, update: Array[Float]): Int = {
    var grads: Array[Float] = inputUpdates.get(nodeId)
    if (grads == null) {
      grads = new Array[Float](params.embeddingDim)
      inputUpdates.put(nodeId, grads)
      inputUpdateCounter.put(nodeId, 0)
    }
    axpy(grads, update, g)
    inputUpdateCounter.addTo(nodeId, 1)
  }

  def axpy(y: Array[Float], x: Array[Float], a: Float): Unit = {
    x.indices.foreach(i => y(i) += a * x(i))
  }

  def div(x: Array[Float], f: Float): Unit = {
    x.indices.foreach(i => x(i) = x(i) / f)
  }

  /* time calculate functions */
  def incPullTime(startTs: Long): Unit = {
    totalPullTime = totalPullTime + (System.currentTimeMillis() - startTs)
  }

  def incPushTime(startTs: Long): Unit = {
    totalPushTime = totalPushTime + (System.currentTimeMillis() - startTs)
  }

  def incCalTime(startTs: Long): Unit = {
    totalCalTime = totalCalTime + (System.currentTimeMillis() - startTs)
  }

  def incCalUpdateTime(startTs: Long): Unit = {
    totalMakeGradTime = totalMakeGradTime + (System.currentTimeMillis() - startTs)
  }

  def incCallNum(): Unit = {
    totalCallNum = totalCallNum + 1
  }

  def avgPullTime: Long = {
    totalPullTime / totalCallNum
  }

  def avgPushTime: Long = {
    totalPushTime / totalCallNum
  }

  def avgCalUpdateTime: Long = {
    totalMakeGradTime / totalCallNum
  }

  def avgCalTime: Long = {
    totalCalTime / totalCallNum
  }

  def logFlag(step: Int): Boolean = {
    totalCallNum % step == 0
  }

  def load(modelPath: String): Unit = {
    val startTime = System.currentTimeMillis()
    LogUtils.logTime(s"load model from $modelPath")

    val loadContext = new ModelLoadContext(modelPath)
    loadContext.addMatrix(new MatrixLoadContext(psMatrix.name))
    PSContext.getOrCreate(SparkContext.getOrCreate()).load(loadContext)
    LogUtils.logTime(s"model load time=${System.currentTimeMillis() - startTime} ms")
  }

  def merge(updateCounter: Int2IntOpenHashMap, updates: Int2ObjectOpenHashMap[Array[Float]],
            nodeId: Int, update: Array[Float], learningRate: Float): Int = {
    var grads: Array[Float] = updates.get(nodeId)
    if (grads == null) {
      grads = new Array[Float](params.embeddingDim)
      updates.put(nodeId, grads)
      updateCounter.put(nodeId, 0)
    }
    update.indices.foreach(i => grads(i) += -1.0f * learningRate * update(i))
    updateCounter.addTo(nodeId, 1)
  }

  def incMakeParamTime(startTs: Long): Unit = {
    totalMakeParamTime = totalMakeParamTime + (System.currentTimeMillis() - startTs)
  }

  def avgMakeParamTime: Long = {
    totalMakeParamTime / totalCallNum
  }
}


object Word2VecModel {

  def parseBatchData(sentences: Array[Array[Int]], windowSize: Int, negative: Int, maxIndex: Int, minIndex: Int,
                     nodeTypeMatrix: PSVector=null, seed: Int = Random.nextInt): (Array[Int], Array[Int], Array[Array[Int]]) = {
    val rand = new Random(seed)
    val srcNodes = new ArrayBuffer[Int]()
    val dstNodes = new ArrayBuffer[Int]()
    for (s <- sentences.indices) {
      val sen = sentences(s)
      for (srcIndex <- sen.indices) {
        var dstIndex = Math.max(srcIndex - windowSize, 0)
        while (dstIndex < Math.min(srcIndex + windowSize + 1, sen.length)) {
          if (srcIndex != dstIndex) {
            srcNodes.append(sen(dstIndex))
            dstNodes.append(sen(srcIndex))
          }
          dstIndex += 1
        }
      }
    }
    if (nodeTypeMatrix == null) {
      val negativeSamples = negativeSample(srcNodes.toArray, dstNodes.toArray, negative,
        maxIndex, minIndex, rand.nextInt())
      (srcNodes.toArray, dstNodes.toArray, negativeSamples)
    } else {
      val start = System.currentTimeMillis()
      val negativeSamples = negativeSampleWithType(srcNodes.toArray, dstNodes.toArray, negative, nodeTypeMatrix, rand.nextInt())
      println("negativeSample with type cost: " + (System.currentTimeMillis()-start))
      (srcNodes.toArray, dstNodes.toArray, negativeSamples)
    }
  }

  def negativeSample(srcNodes: Array[Int], dstNodes: Array[Int], sampleNum: Int, maxIndex: Int,
                     minIndex: Int, seed: Int): Array[Array[Int]] = {
    val rand = new Random(seed)
    val sampleWords = new Array[Array[Int]](srcNodes.length)
    var wordIndex: Int = 0

    for (i <- srcNodes.indices) {
      var sampleIndex: Int = 0
      sampleWords(wordIndex) = new Array[Int](sampleNum)
      while (sampleIndex < sampleNum) {
        val target = rand.nextInt(maxIndex - minIndex) + minIndex
        if (target != dstNodes(i)) {
          sampleWords(wordIndex)(sampleIndex) = target
          sampleIndex += 1
        }
      }
      wordIndex += 1
    }
    sampleWords
  }

  def negativeSampleWithType(srcNodes: Array[Int], dstNodes: Array[Int], sampleNum: Int, nodeTypeMatrix: PSVector, seed: Int): Array[Array[Int]] = {
    val rand = new Random(seed)
    val nodes = srcNodes.distinct
    var start = System.currentTimeMillis()
    val types = nodeTypeMatrix.asyncPull(nodes).get().asInstanceOf[IntIntVector].get(nodes)
    println("pull nodes type cost: " + (System.currentTimeMillis()-start))
    val node2Type = new Int2IntOpenHashMap(nodes, types)
    val type2NodeList = new Int2ObjectOpenHashMap[IntArrayList]()
    types.zip(nodes).map{ case (nodeType: Int, nodeId: Int) =>
      if(!type2NodeList.containsKey(nodeType)) {
        val nodeSet = new IntArrayList()
        nodeSet.add(nodeId)
        type2NodeList.put(nodeType, nodeSet)
      } else {
        type2NodeList.get(nodeType).add(nodeId)
      }
    }
    val sampleWords = new Array[Array[Int]](srcNodes.length)
    var wordIndex: Int = 0
    start = System.currentTimeMillis()
    for (i <- srcNodes.indices) {
      var sampleIndex: Int = 0
      sampleWords(wordIndex) = new Array[Int](sampleNum)
      val dstNodeType = node2Type.get(dstNodes(i))
      val dstNodeSet = type2NodeList.get(dstNodeType)
      var count = 0
      while (sampleIndex < sampleNum) {
        val target = dstNodeSet.getInt(rand.nextInt(dstNodeSet.size()))
        count = count + 1
        if (target != dstNodes(i)) {
          sampleWords(wordIndex)(sampleIndex) = target
          sampleIndex += 1
          count = 0
        } else if (dstNodeSet.size() < 5 || count > 20) {
          sampleWords(wordIndex)(sampleIndex) = srcNodes(rand.nextInt(srcNodes.length))
          sampleIndex += 1
          count = 0
        }
      }
      wordIndex += 1
    }
    println("local negative sampling cost: " + (System.currentTimeMillis()-start))
    sampleWords
  }

}
