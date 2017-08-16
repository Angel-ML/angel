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
import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.context.PSContext

class DenseMatrix(
    override val rows: Int,
    override val columns: Int,
    override val meta: MatrixMeta) extends PSMatrix {

  /**
   * Operations for the whole matrix
   */
  /**
   * Push local matrix to PS
   * @param array local matrix
   */
  def push(array: Array[Array[Double]]): Unit = {
    require(rows == array.length && columns == array(0).length,
      "matrix dimension does not match!")
    (0 until rows).foreach { rowId =>
      super.push(rowId, array(rowId))
    }
  }

  /**
   * Pull PS matrix to local.
   * @return local matrix
   */
  def pull(): Array[Array[Double]] = {
    PSClient().pull(this)
  }

  /**
   * Increment a local matrix to PSMatrix
   * @param array local matrix
   */
  def increment(array: Array[Array[Double]]): Unit = {
    require(rows == array.length && columns == array(0).length,
      "matrix dimension does not match!")
    (0 until rows).foreach { rowId =>
      super.increment(rowId, array(rowId))
    }
  }
}

object DenseMatrix {
  def apply(rows: Int, cols: Int): DenseMatrix = {
    val psContext = PSContext.getOrCreate()
    val matrixMeta = psContext.createMatrix(rows, cols, MatrixType.DENSE)
    new DenseMatrix(rows, cols, matrixMeta)
  }

  /**
   * Create Matrix full of zero.
   */
  def zero(rows: Int, cols: Int): DenseMatrix = {
    DenseMatrix(rows, cols)
  }

  /**
   * Matrix of random elements from 0 to 1
   */
  def rand(rows: Int, cols: Int): DenseMatrix = {
    val mat = DenseMatrix(rows, cols)
    PSClient().random(mat)
    mat
  }

  /**
   * Create identity matrix
   */
  def eye(dim: Int): DenseMatrix = {
    val mat = DenseMatrix(dim, dim)
    PSClient().eye(mat)
    mat
  }

  /**
   * Create diagonal matrix
   */
  def diag(array: Array[Double]): DenseMatrix = {
    val dim = array.length
    val mat = DenseMatrix(dim, dim)
    PSClient().diag(mat, array)
    mat
  }

  /**
   * Create a matrix filled with `x`
   */
  def fill(rows: Int, cols: Int, x: Double): DenseMatrix = {
    val mat = DenseMatrix(rows, cols)
    PSClient().fill(mat, x)
    mat
  }

}
