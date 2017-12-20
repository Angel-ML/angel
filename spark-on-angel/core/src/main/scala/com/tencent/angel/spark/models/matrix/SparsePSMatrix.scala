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

class SparsePSMatrix(
    rows: Int,
    columns: Long,
    meta: MatrixMeta) extends PSMatrix(rows, columns, meta) {
  /**
   * Update specific elements in Matrix.
    *
    * @param pairs is a Array of Tuples[rowId, colsId, value]
   */
  def push(pairs: Array[(Int, Long, Double)]): Unit = {
    psClient.matrixOps.push(this, pairs)
  }

  def pull(): Array[(Int, Long, Double)] = ???


//  override def pull(rowId: Int): Array[(Long, Double)] = ???

  def nnz: Long = ???


  def increment(pairs: Array[(Int, Long, Double)]): Unit = {
    psClient.matrixOps.increment(this, pairs)
  }
}

object SparsePSMatrix {
  val psContext = PSContext.instance()

  def apply(rows: Int, cols: Long): SparsePSMatrix = {
    val matrixMeta = psContext.createMatrix(rows, cols, MatrixType.SPARSE, -1, -1)
    new SparsePSMatrix(rows, cols, matrixMeta)
  }
}