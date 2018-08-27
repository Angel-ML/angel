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


package com.tencent.angel.spark.ml.embedding.line

import scala.util.Random

import org.apache.spark.rdd.RDD

import com.tencent.angel.ml.matrix.psf.get.base.GetFunc
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc
import com.tencent.angel.spark.ml.embedding.NEModel.NEDataSet
import com.tencent.angel.spark.ml.embedding.line.LINEModel.{LINEDataSet, asLineBatch, buildDataBatches}
import com.tencent.angel.spark.ml.embedding.{NEModel, Param}
import com.tencent.angel.spark.ml.psf.embedding.line.{Adjust, Dot}

class LINEModel(numNode: Int,
                dimension: Int,
                numPart: Int,
                numNodesPerRow: Int = -1,
                order: Int = 2,
                seed: Int = Random.nextInt)
  extends NEModel(numNode, dimension, numPart, numNodesPerRow, order, seed) {

  def this(param: Param) {
    this(param.maxIndex, param.embeddingDim, param.numPSPart, param.nodesNumPerRow, param.order, param.seed)
  }

  def train(trainSet: RDD[(Int, Int)], params: Param, validSetOpt: Option[RDD[(Int, Int)]]): this.type = {
    train(trainBatchesRDDIter = buildDataBatches(trainSet, params.batchSize),
      validBatchesOpt = validSetOpt.map(_.mapPartitions(asLineBatch(_, params.batchSize))),
      ns = params.negSample,
      window = None,
      numEpoch = params.numEpoch,
      lr = params.learningRate,
      modelPath = params.modelPath,
      modelCPInterval = params.modelCPInterval,
      logEveryBatchNum = params.numRowDataSet.map(_ / (100.0 * params.partitionNum * params.batchSize))
    )
  }

  override def getDotPsf(data: NEDataSet, batchSeed: Int, ns: Int, window: Option[Int]): GetFunc = {
    val lineData = data.asInstanceOf[LINEDataSet]
    new Dot(matrixId, lineData.src, lineData.dst, batchSeed, ns, numNode, partDim, order)
  }

  override def getAdjustPsf(data: NEDataSet, batchSeed: Int, ns: Int, grad: Array[Float], window: Option[Int])
  : UpdateFunc = {
    val lineData = data.asInstanceOf[LINEDataSet]
    new Adjust(matrixId, lineData.src, lineData.dst, batchSeed, ns, numNode, partDim, grad, order)
  }
}

object LINEModel {

  def buildDataBatches(trainSet: RDD[(Int, Int)], batchSize: Int): Iterator[RDD[NEDataSet]] = {
    new Iterator[RDD[NEDataSet]] with Serializable {
      override def hasNext(): Boolean = true

      override def next(): RDD[NEDataSet] = {
        trainSet.mapPartitions { iter =>
          val shuffledIter = Random.shuffle(iter)
          asLineBatch(shuffledIter, batchSize)
        }
      }
    }
  }

  def asLineBatch(iter: Iterator[(Int, Int)], batchSize: Int): Iterator[NEDataSet] = {
    val src = new Array[Int](batchSize)
    val dst = new Array[Int](batchSize)
    new Iterator[NEDataSet] {
      override def hasNext: Boolean = iter.hasNext

      override def next(): NEDataSet = {
        var pos = 0
        while (iter.hasNext && pos < batchSize) {
          val (s, d) = iter.next()
          src(pos) = s
          dst(pos) = d
          pos += 1
        }
        if (pos < batchSize) LINEDataSet(src.take(pos), dst.take(pos)) else LINEDataSet(src, dst)
      }
    }
  }

  case class LINEDataSet(src: Array[Int], dst: Array[Int]) extends NEDataSet

}
