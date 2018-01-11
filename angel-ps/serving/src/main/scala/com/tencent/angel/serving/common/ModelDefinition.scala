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

import com.tencent.angel.exception.AngelException
import com.tencent.angel.serving.Util

/**
  * the model definition
  *
  * @param name        the model name
  * @param dir         the model directory
  * @param replica     the model replica
  * @param concurrent  the model serving concurrent capacity
  * @param matrixMetas the matrix metas of model
  * @param splits      the model splits
  */
class ModelDefinition(val name: String, val dir: String, val replica: Int, val concurrent: Int, val matrixMetas: Array[MatrixMeta], val splits: Array[ReplicaModelSplit]) {


  /**
    * validate splits conformed to matrix metas
    */
  def validate(): Unit = {
    val matrixMetaMap = matrixMetas.map(matrix => (matrix.name, matrix)).toMap

    splits.flatMap(modelSplit => modelSplit.matrixSplits.map(matrixSplit => (matrixSplit._1, matrixSplit._2.rowOffset, matrixSplit._2.rowNum, matrixSplit._2.columnOffset, matrixSplit._2.dimension)))
      .groupBy(_._1).map { case (matrixName, splits) => (matrixName, splits.map(split => (split._2, split._3)), splits.map(split => (split._4, split._5))) }
      .map(matrixSplitOffsets => (matrixSplitOffsets._1, Util.sum(matrixSplitOffsets._2), Util.sum(matrixSplitOffsets._3)))
      .find {
        case (name, row, dimension) => {
          val matrixMeta = matrixMetaMap(name)
          matrixMeta.rowNum != row || matrixMeta.dimension != dimension
        }
      }.foreach { case (name, row, dimension) => throw new AngelException(s"matrix $name row:$row dimension:$dimension is not match $matrixMetaMap($name)") }

  }

}

