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


package com.tencent.angel.spark.ml.embedding.word2vec

import com.tencent.angel.ml.matrix.psf.get.base.GetFunc
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.embedding.NEModel.NEDataSet
import com.tencent.angel.spark.ml.embedding.word2vec.Word2VecModel.{W2VDataSet, buildDataBatches}
import com.tencent.angel.spark.ml.embedding.{NEModel, Param, Sigmoid}
import com.tencent.angel.spark.ml.psf.embedding.cbow._
import org.apache.spark.rdd.RDD

import scala.util.Random


class Word2VecModel(numNode: Int,
                    dimension: Int,
                    numPart: Int,
                    numNodesPerRow: Int = -1,
                    seed: Int = Random.nextInt,
                    concurrentLevel: Int = 3)
  extends NEModel(numNode, dimension, numPart, numNodesPerRow, 2, false, seed) {

  def this(param: Param) {
    this(param.maxIndex, param.embeddingDim, param.numPSPart, param.nodesNumPerRow, param.seed)
  }

  def train(corpus: RDD[Array[Int]], param: Param): Unit = {

    val numPartitions = corpus.getNumPartitions

    def sgdForBatch(partitionId: Int,
                    threadId: Int,
                    batch: NEDataSet,
                    batchId: Int): (Double, Array[Long]) = {
      var (start, end) = (0L, 0L)

      // dot
      start = System.currentTimeMillis()
      val dotParam = new CbowDotParam(matrixId,
        seed,
        param.negSample,
        param.windowSize,
        partDim,
        partitionId,
        threadId,
        batch.asInstanceOf[W2VDataSet].sentences)

      val dots = psMatrix.psfGet(new CbowDot(dotParam))
        .asInstanceOf[CbowDotResult]
        .getValues
      end = System.currentTimeMillis()
      val dotTime = end - start

      // gradient
      start = System.currentTimeMillis()
      val loss = doGrad(dots,
        param.negSample,
        param.learningRate,
        Some(batch))
      end = System.currentTimeMillis()
      val gradientTime = end - start

      // adjust
      start = System.currentTimeMillis()
      val adjustParam = new CbowAdjustParam(matrixId,
        seed, param.negSample,
        param.windowSize,
        partDim,
        partitionId,
        threadId,
        dots,
        batch.asInstanceOf[W2VDataSet].sentences)
      psMatrix.psfUpdate(new CbowAdjust(adjustParam)).get()
      end = System.currentTimeMillis()
      val adjustTime = end - start

      // return loss
      if (batchId % 100 == 0)
        println(s"batchId=$batchId dotTime=$dotTime gradientTime=$gradientTime adjustTime=$adjustTime")
      (loss, Array(dotTime, gradientTime, adjustTime))
    }

    def sgdForPartition(partitionId: Int,
                        iterator: Iterator[NEDataSet],
                        epoch: Int): Iterator[(Double, Array[Long])] = {
      PSContext.instance()

      if (epoch == 0) {
        val initFunc = getInit(partitionId, numPartitions, numNode)
        psMatrix.psfUpdate(initFunc).get()
      }

      iterator.zipWithIndex.sliding(concurrentLevel, concurrentLevel).map { case seq =>
        seq.foreach{ case (_, index) =>
          if (index % 1000 == 0) NEModel.logTime(s"finish batch $index for epoch $epoch")}
        seq.zipWithIndex.par.map(batch =>
          sgdForBatch(partitionId,
            batch._2,
            batch._1._1,
            batch._1._2))
      }.flatMap(f => f)
    }

    val iterator = buildDataBatches(corpus, param.batchSize)

    for (epoch <- 0 until param.numEpoch) {
      val data = iterator.next()
      val middle = data.mapPartitionsWithIndex(
        (partitionId, iterator) => sgdForPartition(partitionId, iterator, epoch),
        true).collect()
      val loss = middle.map(f => f._1).sum
      val array = new Array[Long](3)
      middle.foreach(f => f._2.zipWithIndex.foreach(t => array(t._2) += t._1))
      println(s"epoch=$epoch " +
        s"loss=$loss " +
        s"dotTime=${array(0)} " +
        s"gradientTime=${array(1)} " +
        s"adjustTime=${array(2)}")
    }

  }

  def getInit(partitionId: Int, numPartitions: Int, maxIndex: Int): UpdateFunc = {
    val param = new CbowInitParam(matrixId, partitionId, numPartitions, maxIndex, concurrentLevel)
    new CbowInit(param)
  }


  override def getDotFunc(data: NEDataSet, batchSeed: Int, ns: Int, window: Option[Int]): GetFunc = ???

  override def getAdjustFunc(data: NEDataSet, batchSeed: Int, ns: Int, grad: Array[Float], window: Option[Int]): UpdateFunc = ???

  override def doGrad(dots: Array[Float],
                      negative: Int,
                      alpha: Float,
                      data: Option[NEDataSet]): Double = {
    val sentences = data.get.asInstanceOf[W2VDataSet].sentences
    val size = sentences.map(sen => sen.length).sum
    var label = 0
    var sumLoss = 0f
    assert(dots.length == size * (negative + 1))
    for (a <- 0 until dots.length) {
      val sig = Sigmoid.sigmoid(dots(a))
      if (a % (negative + 1) == 0) { // positive target
        sumLoss += -sig
        dots(a) = (1 - sig) * alpha
      } else { // negative target
        label = 0
        sumLoss += -Sigmoid.sigmoid(-dots(a))
        dots(a) = -sig * alpha
      }
    }
    sumLoss
  }
}

object Word2VecModel {

  def buildDataBatches(trainSet: RDD[Array[Int]], batchSize: Int): Iterator[RDD[NEDataSet]] = {
    new Iterator[RDD[NEDataSet]] with Serializable {
      override def hasNext(): Boolean = true

      override def next(): RDD[NEDataSet] = {
        trainSet.mapPartitions { iter =>
          val shuffledIter = Random.shuffle(iter)
          asWord2VecBatch(shuffledIter, batchSize)
        }
      }
    }
  }

  def asWord2VecBatch(iter: Iterator[Array[Int]], batchSize: Int): Iterator[NEDataSet] = {
    val sentences = new Array[Array[Int]](batchSize)
    new Iterator[NEDataSet] {
      override def hasNext: Boolean = iter.hasNext

      override def next(): NEDataSet = {
        var pos = 0
        while (iter.hasNext && pos < batchSize) {
          sentences(pos) = iter.next()
          pos += 1
        }
        if (pos < batchSize) W2VDataSet(sentences.take(pos)) else W2VDataSet(sentences)
      }
    }
  }

  case class W2VDataSet(sentences: Array[Array[Int]]) extends NEDataSet

}
