/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 *  Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *  https://opensource.org/licenses/BSD-3-Clause
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the License
 *  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 *  or implied. See the License for the specific language governing permissions and limitations under
 *  the License.
 *
 */

package com.tencent.angel.serving.client

import com.tencent.angel.ml.math.TVector
import com.tencent.angel.serving._
import com.tencent.angel.serving.common._
import com.tencent.angel.serving.transport.serving.ServingTransportClient

import scala.util.Random

class DefaultModelRouter(val name: String, servingClient: ServingClient, transportClient: ServingTransportClient) extends ModelRouter {

  def getLocations(model: DistributedModel): Array[(Map[String, MatrixSplit], Array[ServingLocation])] = {
    model.splits.map(modelSplit => (modelSplit.matrixSplits, modelSplit.replica.locations))
  }

  override def route[V <: TVector](data: PredictData[V]): Array[PredictSplitData[V]] = {
    val model = getModel                  // DistributedModel
    val modelLocs = getLocations(model)   // Array[(Map[String, MatrixSplit], Array[ServingLocation])]
    splitPredictData(model, data).zipWithIndex.map{ case (chunkedShardingData, idx) =>
      val locs = modelLocs(idx)._2       // Array[ServingLocation]
      val shuffledLocs = Random.shuffle(locs.toList).toArray
      new PredictSplitData(new ModelSplitID(model.name, idx), shuffledLocs, chunkedShardingData)
    }
  }


  private def getModel: DistributedModel = {
    val model = servingClient.getModel(name).orNull
    require(model != null, s"the model $name not found")
    require(model.isServable, s"${model.name} is not ready for serving")
    model
  }

  //TODO
  def splitData[V <: TVector](model: DistributedModel, data: PredictData[V]): Array[ChunkedShardingData[V]] = {
    val splits = model.splits
    val splitsOffsets = splits.map(split => {
      split.matrixSplits.values.map(matrixSplit => (matrixSplit.columnOffset, matrixSplit.dimension)).toArray
    })

    splitsOffsets.map(offset => offset.sortWith((offset1, offset2) => {
      offset1._1 <= offset2._1 || offset1._2 >= offset2._2
    }))
    null
  }

  def splitPredictData[V <: TVector](model: DistributedModel, data: PredictData[V]): Array[ChunkedShardingData[V]] = {
    val splitsOffsets = model.splits.map{split =>
      split.matrixSplits.values.map{matrixSplit => (matrixSplit.columnOffset, matrixSplit.dimension)}.toArray
    }

    val shardingData = new SingleShardingData[V](data.x)

    splitsOffsets.map{offsets =>
      val chunk = ChunkedShardingData(data.x.getType).asInstanceOf[ChunkedShardingData[V]]
      offsets.foreach { case (offset, dimension) =>
        chunk.insert(offset, shardingData.getData(offset, dimension))
      }
      chunk
    }
  }

  override def predict[V <: TVector](data: PredictSplitData[V]): PredictResult = {
    transportClient.predict(data)
  }
}
