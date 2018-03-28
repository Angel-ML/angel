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

import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.vector.DensePSVector

class DensePSMatrix(
    id: Int,
    rows: Int,
    columns: Int) extends PSMatrix(id, rows, columns) {

  /**
   * Operations for the whole matrix
   */
  /**
   * Push local matrix to PS
   * @param array local matrix
   */
  def push(array: Array[Array[Double]]): Unit = {
    assertValid()
    require(rows == array.length && columns == array(0).length,
      "matrix dimension does not match!")
    (0 until rows).foreach { rowId =>
      push(rowId, array(rowId))
    }
  }

  /**
   * Push a array to `rowId` in matrix
   */
  def push(rowId: Int, array: Array[Double]) = {
    assertValid()
    PSClient.instance().denseRowOps.push(getRow(rowId), array)
  }

  /**
   * Pull PS matrix to local.
   * @return local matrix
   */
  def pull(): Array[Array[Double]] = {
    assertValid()
    PSClient.instance().matrixOps.pull(this)
  }

  /**
   * Pull multi rows to local
   */
  def pull(rows: Array[Int]): Array[(Int, Array[Double])] = {
    assertValid()
    PSClient.instance().matrixOps.pull(this, rows)
  }

  /**
   * Pull a row to local from PS
   */
  def pull(rowId: Int): Array[Double] = {
    assertValid()
    PSClient.instance().denseRowOps.pull(getRow(rowId)).values
  }

  /**
   * Increment a local matrix to PSMatrix
   * @param array local matrix
   */
  def increment(array: Array[Array[Double]]): Unit = {
    assertValid()
    require(rows == array.length && columns == array(0).length,
      "matrix dimension does not match!")
    (0 until rows).foreach { rowId =>
      increment(rowId, array(rowId))
    }
  }

  /**
   * Increment a row of matrix with `array`
   */
  def increment(rowId: Int, array: Array[Double]) = {
    assertValid()
    PSClient.instance().denseRowOps.increment(getRow(rowId), array)
  }


  private def getRow(rowId: Int): DensePSVector = {
    new DensePSVector(id, rowId, columns.toInt)
  }
}

object DensePSMatrix {
  def apply(rows: Int, cols: Int): DensePSMatrix = {
    apply(rows, cols, -1, -1)
  }

  def apply(rows: Int, cols: Int, rowInBlock: Int, colInBlock: Int): DensePSMatrix = {
    val matrixMeta = PSContext.instance().createDenseMatrix(rows, cols, rowInBlock, colInBlock)
    new DensePSMatrix(matrixMeta.getId, rows, cols)
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
    PSClient.instance().matrixOps.random(mat)
    mat
  }

  /**
   * Create identity matrix
   */
  def eye(dim: Int): DensePSMatrix = {
    val mat = DensePSMatrix(dim, dim)
    PSClient.instance().matrixOps.eye(mat)
    mat
  }

  /**
   * Create diagonal matrix
   */
  def diag(array: Array[Double]): DensePSMatrix = {
    val dim = array.length
    val mat = DensePSMatrix(dim, dim)
    PSClient.instance().matrixOps.diag(mat, array)
    mat
  }

  /**
   * Create a matrix filled with `x`
   */
  def fill(rows: Int, cols: Int, x: Double): DensePSMatrix = {
    val mat = DensePSMatrix(rows, cols)
    PSClient.instance().matrixOps.fill(mat, x)
    mat
  }

}
