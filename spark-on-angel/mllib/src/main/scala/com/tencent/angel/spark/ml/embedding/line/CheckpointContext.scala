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

import java.util

import com.tencent.angel.spark.models.PSMatrix

/**
  * Checkpoint context
  */
class CheckpointContext extends Serializable {
  /**
    * Read and write ps matrices
    */
  val readWriteMatrices = new util.ArrayList[PSMatrix]()

  /**
    * Read only ps matrices
    */
  val readOnlyMatrices = new util.ArrayList[PSMatrix]()

  def addReadWriteMatrix(matrix:PSMatrix): Boolean = readWriteMatrices.add(matrix)

  def addReadOnlyMatrix(matrix:PSMatrix): Boolean = readOnlyMatrices.add(matrix)

}
