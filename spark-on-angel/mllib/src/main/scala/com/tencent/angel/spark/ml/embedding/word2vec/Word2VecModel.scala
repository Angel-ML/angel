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

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc
import com.tencent.angel.spark.ml.embedding.NEModel.NEDataSet
import com.tencent.angel.spark.ml.embedding.word2vec.Word2VecModel.{W2VDataSet, buildDataBatches}
import com.tencent.angel.spark.ml.embedding.{FastSigmoid, NEModel, Param}
import com.tencent.angel.spark.ml.psf.embedding.w2v._
import org.apache.spark.rdd.RDD

import scala.util.Random


class Word2VecModel(numNode: Int,
                    dimension: Int,
                    numPart: Int,
                    maxLength: Int,
                    model: String,
                    numNodesPerRow: Int = -1,
                    seed: Int = Random.nextInt)
  extends NEModel(numNode, dimension, numPart, numNodesPerRow, 2, false, seed) {

  def this(param: Param) {
    this(param.maxIndex, param.embeddingDim, param.numPSPart, param.maxLength, param.model)
  }

  val modelId = model match {
    case "skipgram" => 0
    case "cbow" => 1
    case _ => throw new AngelException("model type should be cbow or skipgram")
  }

  def train(corpus: RDD[Array[Int]], param: Param): Unit = {
    psMatrix.psfUpdate(getInitFunc(corpus.getNumPartitions, numNode, maxLength))
    val iterator = buildDataBatches(corpus, param.batchSize)
    train(iterator, None, param.negSample,
      Some(param.windowSize), param.numEpoch, param.learningRate,
      Some(maxLength), param.checkpointInterval)
  }

  def getInitFunc(numPartitions: Int, maxIndex: Int, maxLength: Int): UpdateFunc = {
    val param = new InitParam(matrixId, numPartitions, maxIndex, maxLength)
    new Init(param)
  }


  override def getDotFunc(data: NEDataSet, batchSeed: Int, ns: Int, window: Option[Int],
                          partitionId: Option[Int]): GetFunc = {
    val param = new DotParam(matrixId,
      seed, ns,
      window.get,
      partDim,
      partitionId.get,
      modelId,
      data.asInstanceOf[W2VDataSet].sentences)
    new Dot(param)
  }

  override def getAdjustFunc(data: NEDataSet, batchSeed: Int, ns: Int, grad: Array[Float],
                             window: Option[Int], partitionId: Option[Int]): UpdateFunc = {
    val param = new AdjustParam(matrixId,
      seed, ns,
      window.get,
      partDim,
      partitionId.get,
      modelId,
      grad,
      data.asInstanceOf[W2VDataSet].sentences)
    new Adjust(param)
  }

  override def doGrad(dots: Array[Float],
                      negative: Int,
                      alpha: Float,
                      data: Option[NEDataSet]): Double = {
//    val sentences = data.get.asInstanceOf[W2VDataSet].sentences
//    val size = sentences.map(sen => sen.length).sum
    var label = 0
    var sumLoss = 0f
//    assert(dots.length == size * (negative + 1))
    for (a <- 0 until dots.length) {
      val sig = FastSigmoid.sigmoid(dots(a))
      if (a % (negative + 1) == 0) { // positive target
        sumLoss += -sig
        dots(a) = (1 - sig) * alpha
      } else { // negative target
        label = 0
        sumLoss += -FastSigmoid.sigmoid(-dots(a))
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
