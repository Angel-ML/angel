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

import scala.util.Random

import org.apache.spark.rdd.RDD

import com.tencent.angel.spark.ml.embedding.NEModel.NEDataSet
import com.tencent.angel.spark.ml.embedding.word2vec.Word2VecModel.{W2VDataSet, asWord2VecBatch, buildDataBatches}
import com.tencent.angel.spark.ml.embedding.{NEModel, Param}
import com.tencent.angel.spark.ml.psf.embedding.word2vec.{Adjust, Dot}


class Word2VecModel(numNode: Int,
                    dimension: Int,
                    numPart: Int,
                    numNodesPerRow: Int = -1,
                    seed: Int = Random.nextInt)
  extends NEModel(numNode, dimension, numPart, numNodesPerRow, 2, seed) {

  def this(param: Param) {
    this(param.maxIndex, param.embeddingDim, param.numPSPart, param.nodesNumPerRow, param.seed)
  }

  def train(trainSet: RDD[Array[Int]], params: Param, validateSet: Option[RDD[Array[Int]]]): this.type = {
    train(trainBatchesRDDIter = buildDataBatches(trainSet, params.batchSize),
      validBatchesOpt = validateSet.map(_.mapPartitions(asWord2VecBatch(_, params.batchSize))),
      ns = params.negSample,
      window = Some(params.windowSize),
      numEpoch = params.numEpoch,
      lr = params.learningRate,
      modelPath = params.modelPath,
      modelCPInterval = params.modelCPInterval,
      logEveryBatchNum = params.numRowDataSet.map(_ / (100.0 * params.partitionNum * params.batchSize))
    )
  }

  override def getDotPsf(sentences: NEDataSet, seed: Int, ns: Int, window: Option[Int]) =
    new Dot(matrixId, sentences.asInstanceOf[W2VDataSet].sentences, seed, ns, window.get, numNode, partDim)

  override def getAdjustPsf(sentences: NEDataSet, seed: Int, ns: Int, grad: Array[Float], window: Option[Int]) =
    new Adjust(matrixId, sentences.asInstanceOf[W2VDataSet].sentences, seed, ns, window.get, numNode, partDim, grad)

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
