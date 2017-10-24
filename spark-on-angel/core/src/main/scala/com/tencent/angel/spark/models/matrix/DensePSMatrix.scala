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
import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.context.PSContext

class DensePSMatrix(
    rows: Int,
    columns: Int,
    meta: MatrixMeta) extends PSMatrix(rows, columns, meta) {

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
    psClient.matrixOps.pull(this)
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

object DensePSMatrix {
  val psContext = PSContext.instance()
  val psClient = PSClient.instance()

  def apply(rows: Int, cols: Int): DensePSMatrix = {
    apply(rows, cols, -1, -1)
  }

  def apply(rows: Int, cols: Int, rowInBlock: Int, colInBlock: Int): DensePSMatrix = {
    val matrixMeta = psContext.createMatrix(rows, cols, MatrixType.DENSE, rowInBlock, colInBlock)
    new DensePSMatrix(rows, cols, matrixMeta)
  }

  /**
   * Create Matrix full of zero.
   */
  def zero(rows: Int, cols: Int): DensePSMatrix = {
    DensePSMatrix(rows, cols)
  }

  /**
   * Matrix of random elements from 0 to 1
   */
  def rand(rows: Int, cols: Int): DensePSMatrix = {
    val mat = DensePSMatrix(rows, cols)
    psClient.initOps.random(mat)
    mat
  }

  /**
   * Create identity matrix
   */
  def eye(dim: Int): DensePSMatrix = {
    val mat = DensePSMatrix(dim, dim)
    psClient.matrixOps.eye(mat)
    mat
  }

  /**
   * Create diagonal matrix
   */
  def diag(array: Array[Double]): DensePSMatrix = {
    val dim = array.length
    val mat = DensePSMatrix(dim, dim)
    psClient.matrixOps.diag(mat, array)
    mat
  }

  /**
   * Create a matrix filled with `x`
   */
  def fill(rows: Int, cols: Int, x: Double): DensePSMatrix = {
    val mat = DensePSMatrix(rows, cols)
    psClient.matrixOps.fill(mat, x)
    mat
  }

}
