/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.spark.models.matrix

import com.tencent.angel.ml.matrix.MatrixMeta
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.linalg.SparseVector
import com.tencent.angel.spark.models.vector.SparsePSVector

class SparsePSMatrix(
    id: Int,
    rows: Int,
    columns: Long) extends PSMatrix(id, rows, columns) {
  /**
   * Update specific elements in Matrix.
    *
    * @param pairs is a Array of Tuples[rowId, colsId, value]
   */
  def push(pairs: Array[(Int, Long, Double)]): Unit = {
    psClient.matrixOps.push(this, pairs)
  }

  def pull(): Array[(Int, Long, Double)] = ???


  def pull(rowId: Int): SparseVector = {
    psClient.sparseRowOps.pull(getRow(rowId))
  }

  def increment(pairs: Array[(Int, Long, Double)]): Unit = {
    psClient.matrixOps.increment(this, pairs)
  }

  private def getRow(rowId: Int): SparsePSVector = {
    new SparsePSVector(id, rowId, columns)
  }
}

object SparsePSMatrix {
  val psContext = PSContext.instance()

  def apply(rows: Int, cols: Long, range: Long): SparsePSMatrix = {
    val matrixMeta = psContext.createSparseMatrix(rows, cols, range, -1, -1)
    new SparsePSMatrix(matrixMeta.getId, rows, cols)
  }

  def apply(rows: Int, cols: Long): SparsePSMatrix = {
    val matrixMeta = psContext.createSparseMatrix(rows, cols, cols, -1, -1)
    new SparsePSMatrix(matrixMeta.getId, rows, cols)
  }
}