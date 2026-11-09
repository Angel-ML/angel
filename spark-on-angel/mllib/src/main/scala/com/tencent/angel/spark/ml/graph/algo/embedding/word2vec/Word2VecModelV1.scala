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


package com.tencent.angel.spark.ml.graph.algo.embedding.word2vec


import com.tencent.angel.ml.math2.vector.IntIntVector
import com.tencent.angel.spark.ml.graph.algo.embedding.line2.{LINEModelRandomize, RandomizeUpdateParam}
import com.tencent.angel.spark.ml.util.LogUtils
import com.tencent.angel.spark.models.PSVector
import it.unimi.dsi.fastutil.ints.{Int2ObjectOpenHashMap, IntArrayList}
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import scala.util.Random

class Word2VecModelV1(params: Word2VecParam) extends Word2VecModel(params) {

  override def train(corpus: RDD[(Int, Int)], arrayBoundsFromPS: Array[Int], cols: Int, kernel: Int): Unit = {
    var learningRate = params.learningRate
    val startTs = System.currentTimeMillis()
    // Before training, checkpoint the model
    psMatrix.checkpoint(0)
    LogUtils.logTime(s"Write checkpoint use time=${System.currentTimeMillis() - startTs}")
    for (epoch <- 1 to params.numEpoch) {
      val epochStartTime = System.currentTimeMillis()
      val (lossSum, size) = corpus.mapPartitionsWithIndex { case (index, iterator) =>
        val (maxIndex, minIndex) = if (modelContext.isUseRangePartition) {
          val psIndex = index % cols * kernel
          val minIndex = if (psIndex > 0) arrayBoundsFromPS(psIndex - 1) else params.minIndex
          val maxIndex = if (arrayBoundsFromPS.length > 0) arrayBoundsFromPS(psIndex + kernel - 1) else params.maxIndex
          (maxIndex, minIndex)
        } else {
          (params.maxIndex, params.minIndex)
        }
        iterator.sliding(params.batchSize, params.batchSize)
          .map(batch => (optimize(batch.toArray, params.windowSize, params.negSample, maxIndex, minIndex,
            learningRate, params.logStep, kernel), 1))
      }.reduce((f1, f2) => (f1._1 + f2._1, f1._2 + f2._2))
      learningRate = ssScheduler.next().toFloat
      val epochTime = System.currentTimeMillis() - epochStartTime
      println(s"epoch=$epoch lr=$learningRate loss=${lossSum / size} time=${epochTime.toFloat / 1000}s")
      learningRate = Math.max(params.learningRate*0.0001f, ssScheduler.next().toFloat)
      checkpointAndSaveIfNeed(epoch, params)
    }
  }

  def optimize(batch: Array[(Int, Int)], windowSize: Int, numNegSample: Int, maxIndex: Int, minIndex: Int,
               learningRate: Float, logStep: Int, kernel: Int = 1): Double = {
    val trainBatch = Word2VecModelV1.parseBatchData(batch, windowSize, numNegSample, maxIndex, minIndex, params.localSample, nodeTypeMatrix, kernel = kernel)
    val loss = optimizeOneBatch(batch.length, trainBatch._1, trainBatch._2, trainBatch._3, numNegSample, learningRate, logStep)
    loss
  }
}


object Word2VecModelV1 {
  def parseBatchData(sentences: Array[(Int, Int)], windowSize: Int, negative: Int, maxIndex: Int, minIndex: Int, localSample: Boolean,
                     nodeTypeMatrix: PSVector=null, seed: Int = Random.nextInt, kernel: Int = 1): (Array[Int], Array[Int], Array[Array[Int]]) = {
    val rand = new Random(seed)
    val (srcNodes, dstNodes) = sentences.unzip
    if (nodeTypeMatrix == null) {
      val negativeSamples = {
        if (localSample) localNegativeSample(srcNodes, dstNodes, negative, maxIndex, minIndex, rand.nextInt())
        else negativeSample(srcNodes, dstNodes, negative, maxIndex, minIndex, rand.nextInt())
      }
      (srcNodes, dstNodes, negativeSamples)
    } else {
      val negativeSamples = negativeSampleWithType(srcNodes, dstNodes, negative, nodeTypeMatrix, rand.nextInt())
      (srcNodes, dstNodes, negativeSamples)
    }
  }

  def negativeSample(srcNodes: Array[Int], dstNodes: Array[Int], sampleNum: Int, maxIndex: Int,
                     minIndex: Int, seed: Int): Array[Array[Int]] = {
    val rand = new Random(seed)
    val sampleWords = new Array[Array[Int]](srcNodes.length)

    for (i <- srcNodes.indices) {
      var sampleIndex: Int = 0
      sampleWords(i) = new Array[Int](sampleNum)
      while (sampleIndex < sampleNum) {
        val target = rand.nextInt(maxIndex - minIndex) + minIndex
        if (target != srcNodes(i) && target != dstNodes(i)) {
          sampleWords(i)(sampleIndex) = target
          sampleIndex += 1
        }
      }
    }
    sampleWords
  }

  def localNegativeSample(srcNodes: Array[Int], dstNodes: Array[Int], sampleNum: Int, maxIndex: Int,
                          minIndex: Int, seed: Int): Array[Array[Int]] = {
    val rand = new Random(seed)
    val sampleWords = new Array[Array[Int]](srcNodes.length)

    var nodes = new mutable.HashSet[Int]()
    nodes ++= srcNodes
    nodes ++= dstNodes
    val sampleSet = nodes.toArray

    for (i <- srcNodes.indices) {
      var sampleIndex: Int = 0
      val len = sampleSet.length
      sampleWords(i) = new Array[Int](sampleNum)
      if (len < 3) { // in case local sampling is invalid
        while (sampleIndex < sampleNum) {
          val target = rand.nextInt(maxIndex - minIndex) + minIndex
          if (target != srcNodes(i) && target != dstNodes(i)) {
            sampleWords(i)(sampleIndex) = target
            sampleIndex += 1
          }
        }
      } else {
        while (sampleIndex < sampleNum) {
          val target = sampleSet(rand.nextInt(len))
          if (target != srcNodes(i) && target != dstNodes(i)) {
            sampleWords(i)(sampleIndex) = target
            sampleIndex += 1
          }
        }
      }
    }
    sampleWords
  }

  def negativeSampleWithType(srcNodes: Array[Int], dstNodes: Array[Int], sampleNum: Int, nodeTypeMatrix: PSVector, seed: Int): Array[Array[Int]] = {
    val rand = new Random(seed)
    val nodes = srcNodes.union(dstNodes).distinct
    val types = nodeTypeMatrix.asyncPull(nodes).get().asInstanceOf[IntIntVector].get(nodes)
    val node2Type = nodes.zip(types).toMap
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

    for (i <- srcNodes.indices) {
      var sampleIndex: Int = 0
      sampleWords(i) = new Array[Int](sampleNum)
      val dstNodeType = node2Type(dstNodes(i))
      val dstNodeSet = type2NodeList.get(dstNodeType)
      while (sampleIndex < sampleNum) {
        val target = dstNodeSet.getInt(rand.nextInt(dstNodeSet.size()))
        if (target != dstNodes(i)) {
          sampleWords(i)(sampleIndex) = target
          sampleIndex += 1
        }
      }
    }
    sampleWords
  }

}
