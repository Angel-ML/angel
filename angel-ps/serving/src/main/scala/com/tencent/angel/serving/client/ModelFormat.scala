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

import com.tencent.angel.serving.common.{MatrixSplit, ModelMeta, ModelSplit, ReplicaModelSplit}


class ModelFormat(val name: String, val dir: String, val concurrent: Int, val replica: Int, val meta: ModelMeta, splitter: ModelSplitter, coordinator: ModelCoordinator) {

  def getModel(): DistributedModel = {
    new DistributedModel(name, dir, replica, concurrent, splitter.split(meta).map(ReplicaModelSplit(_)), meta.matricesMeta.map(matrixMeta => (matrixMeta.name, matrixMeta)).toMap, coordinator, splitter)
  }

}

trait ModelSplitter {
  def split(meta: ModelMeta): Array[ModelSplit]
}

class DefaultModelSplitter(targetNum: Int) extends ModelSplitter {
  override def split(meta: ModelMeta): Array[ModelSplit] = {
    val matricesMeta = meta.matricesMeta
    // (idx,(columnQuota, lastColumnQuota))
    val splitQuota = matricesMeta.map(matrixMeta => (matrixMeta.dimension > targetNum, matrixMeta.dimension,
      if (matrixMeta.dimension > targetNum) {
        (matrixMeta.dimension + targetNum - 1) / targetNum
      } else {
        matrixMeta.dimension
      }))
      .map { case (isSharding, dimension, quota) => (isSharding, quota,
        if (isSharding && dimension != quota * targetNum) {
          dimension - (quota * (targetNum - 1))
        } else {
          quota
        })
      }.zipWithIndex

    def getModelSplit(idx: Int): ModelSplit = {
      new ModelSplit(idx, splitQuota.map {
        case ((isSharding, columnQuota, lastColumnQuota), matrixIdx) => {
          val matrixMeta = matricesMeta(matrixIdx)
          (matrixMeta.name, new MatrixSplit(matrixMeta.name, idx, 0, matrixMeta.rowNum,
            if (isSharding) {
              idx * columnQuota
            } else {
              0
            }, if (idx == targetNum - 1) lastColumnQuota else columnQuota))
        }
      }.toMap)
    }

    (0 until targetNum).map(idx => getModelSplit(idx)).toArray
  }
}

