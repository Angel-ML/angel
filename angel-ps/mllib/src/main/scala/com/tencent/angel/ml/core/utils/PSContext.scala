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
package com.tencent.angel.ml.core.utils

import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.ml.math2.vector.Vector

trait PSContext {
  def getMatrixId(name: String): Int

  def createMatrixCtx(name: String, numRows: Int, numCols: Long,
                      rowType: RowType, formatClassName: String,
                      partitionClass: String, validIndexNum: Long): MatrixContext

  def createMatrix(ctxs: Iterator[MatrixContext]): Unit

  def createMatrix(ctx: MatrixContext): Unit = {
    createMatrix(Iterator.single(ctx))
  }

  def getRow(epoch: Int, matrixId: Int, rowId: Int, index: Vector): Vector

  def getRow(epoch: Int, matrixId: Int, rowId: Int): Vector = {
    getRow(epoch, matrixId, rowId, null)
  }

  def getRows(epoch: Int, matrixId: Int, startRowId: Int, endRowId: Int, index: Vector): Array[Vector]

  def getRows(epoch: Int, matrixId: Int, startRowId: Int, endRowId: Int): Array[Vector] = {
    getRows(epoch, matrixId, startRowId, endRowId, null)
  }

  def incrementRow(matrixId: Int, rowId: Int, vector: Vector): Unit

  def incrementRows(matrixId: Int, rowIds: Array[Int], vectors: Array[Vector]): Unit

  def updateRow(matrixId: Int, rowId: Int, vector: Vector): Unit

  def updateRows(matrixId: Int, rowIds: Array[Int], vectors: Array[Vector]): Unit
}
