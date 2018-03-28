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

package com.tencent.angel.serving.common

/**
  * the model split
  *
  * @param index        the model split index
  * @param matrixSplits the matrix splits
  */
class ModelSplit(val index: Int, val matrixSplits: Map[String, MatrixSplit]) {

}

/**
  * the model split which has serving locations
  *
  * @param index        the model split index
  * @param matrixSplits the matrix splits
  */
class ReplicaModelSplit(index: Int, matrixSplits: Map[String, MatrixSplit]) extends ModelSplit(index, matrixSplits) {
  val replica: ServingReplica = new ServingReplica()

  def pure: ModelSplit = new ModelSplit(index, matrixSplits)
}

object ReplicaModelSplit {
  def apply(modelSplit: ModelSplit): ReplicaModelSplit = new ReplicaModelSplit(modelSplit.index, modelSplit.matrixSplits)
}

/**
  * the model split group
  * @param name the model name
  * @param dir the model directory
  * @param concurrent the serving concurrent capacity
  * @param splits the model splits
  */
class ModelSplitGroup(val name: String, val dir: String, val concurrent: Int = -1,
                      val splits: Array[ModelSplit], val shardingModelClass: String)
