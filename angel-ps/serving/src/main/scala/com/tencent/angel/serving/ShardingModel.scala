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

package com.tencent.angel.serving

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math.{TMatrix, TVector}


abstract class ShardingModel extends Serializable{
  private[this] var _matrices: Map[String, ShardingMatrix] = _

  def matrices: Map[String, ShardingMatrix] = {
    require(_matrices != null)
    _matrices

  }

  def init(matrices: Map[String, ShardingMatrix]): Unit = {
    _matrices = matrices
  }

  protected def getShardingMatrix(matrixName: String): ShardingMatrix = matrices.getOrElse(matrixName, {
    throw new AngelException(s"matrix:$matrixName not exits");
  })


  protected def getMatrix(matrixName: String): TMatrix[TVector] = {
    getShardingMatrix(matrixName).matrix
  }

  def getRowNum(matrixName: String): Int = {
    getMatrix(matrixName).getRowNum
  }

  def getRowOffset(matrixName: String): Int = {
    getShardingMatrix(matrixName).rowOffset
  }

  def getDimension(matrixName: String): Long = {
    getMatrix(matrixName).getColNum
  }

  def getColumnOffset(matrixName: String): Long = {
    getShardingMatrix(matrixName).columnOffset
  }


  protected def getRow(matrixName: String, idx: Int): TVector = {
    matrices.get(matrixName).map(matrix => matrix.matrix.getRow(idx)).getOrElse(null)
  }

  def load(): Unit

  def predict(data: ShardingData): PredictResult
}
