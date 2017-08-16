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

package com.tencent.angel.spark.model.matrix

import com.tencent.angel.ml.matrix.MatrixMeta
import com.tencent.angel.spark.context.PSContext

class SparseMatrix(
    override val rows: Int,
    override val columns: Int,
    override val meta: MatrixMeta) extends PSMatrix {
  /**
   * Operations for the whole matrix
   */

  /**
   * Update specific elements in Matrix.
    *
    * @param pairs is a Array of Tuples[rowId, colsId, value]
   */
  def push(pairs: Array[(Int, Int, Double)]): Unit = ???


  def increment(pairs: Array[(Int, Int, Double)]): Unit = ???
}

object SparseMatrix {

  def apply(rows: Int, cols: Int): SparseMatrix = {
    val psContext = PSContext.getOrCreate()
    val matrixMeta = psContext.createMatrix(rows, cols, MatrixType.SPARSE)
    new SparseMatrix(rows, cols, matrixMeta)
  }

  /**
   * Create Matrix full of zero.
   */
  def zero(rows: Int, cols: Int): SparseMatrix = ???

  /**
   * Matrix of random elements from 0 to 1
   */
  def rand(rows: Int, cols: Int): SparseMatrix = ???

  /**
   * Create identity matrix
   */
  def eye(dim: Int): SparseMatrix = ???

  /**
   * Create diagonal matrix
   */
  def diag(array: Array[Double]): SparseMatrix = ???

  /**
   * Create a matrix filled with `x`
   */
  def fill(row: Int, cols: Int, x: Double): SparseMatrix = ???


}