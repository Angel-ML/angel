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

import com.tencent.angel.serving.common.{MatrixMeta, ModelDefinition, ReplicaModelSplit}

/**
  * the distributed model which represent a serving unit on client
  *
  * @param name        the model name
  * @param dir         the model directory
  * @param concurrent  the concurrent capacity on serving node
  * @param replica     the model replica
  * @param splits the model splits
  * @param metas the model metas
  * @param coordinator the model coordinator
  * @param splitter the model splitter
  */
class DistributedModel(val name: String,
                       val dir: String,
                       val concurrent: Int,
                       val replica: Int = 1,
                       val splits: Array[ReplicaModelSplit],
                       val metas: Map[String, MatrixMeta],
                       val coordinator: ModelCoordinator,
                       val splitter: ModelSplitter) {


  def isServable(): Boolean = {
    splits.forall(split => split.replica.locations.length != 0)
  }

  def getCoordinator(): ModelCoordinator = {
    coordinator
  }

  def toModelDefinition(): ModelDefinition = {
    new ModelDefinition(name, dir, replica, concurrent, metas.values.toArray, splits)
  }
}
