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


package com.tencent.angel.spark.models.matrix


import java.util.concurrent.Future

import org.apache.commons.logging.LogFactory

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, GetResult}
import com.tencent.angel.ml.matrix.psf.update.Reset
import com.tencent.angel.ml.matrix.psf.update.base.{UpdateFunc, VoidResult}
import com.tencent.angel.psagent.matrix.{MatrixClient, MatrixClientFactory}
import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.PSModel
import com.tencent.angel.spark.util.RowTypeImplicit._

abstract class PSMatrix extends PSModel {
  val id: Int
  val rows: Int
  val columns: Long
  val rowType: RowType
  private var deleted: Boolean = false
  private[matrix] val log = LogFactory.getLog(this.getClass)

  def size: Long = rows * columns

  override def toString: String = {
    s"PSMatrix(id=$id rows=$rows cols=$columns)"
  }

  /*
  Pull PS matrix or vector(s) to local.
   */

  def pull(): Matrix = psClient.matrixOps.pull(this)

  def pull(rowIds: Array[Int], indexes: Array[Long]): Array[Vector] = {
    require(rowType.isLongKey, s"pull with long index is not supported for $rowType")
    matrixClient.get(rowIds, indexes)
  }

  def pull(rowIds: Array[Int], indexes: Array[Int]): Array[Vector] = {
    require(rowType.isIntKey, s"pull with int index is not supported for $rowType")
    matrixClient.get(rowIds, indexes)
  }

  def pull(rowId: Int, indexes: Array[Long]): Vector = {
    require(rowType.isLongKey, s"pull with long index is not supported for $rowType")
    matrixClient.get(rowId, indexes)
  }

  def pull(rowId: Int, indexes: Array[Int]): Vector = {
    require(rowType.isIntKey, s"pull with int index is not supported for $rowType")
    matrixClient.get(rowId, indexes)
  }

  def pull(rowId: Int): Vector = matrixClient.getRow(rowId, true)

  def pull(rowIds: Array[Int]): Array[Vector] = rowIds.indices.par.map(rowId => pull(rowId)).toArray


  /*
   increment local matrix or vector(s) to ps
    */

  def increment(delta: Matrix): Unit = matrixClient.increment(delta, true)

  def increment(delta: Vector): Unit = matrixClient.increment(delta, true)

  def increment(rowId: Int, delta: Vector): Unit = matrixClient.increment(rowId, delta, true)

  def increment(rowIds: Array[Int], deltas: Array[Vector]): Unit =
    matrixClient.increment(rowIds, deltas, true)

  /*
  update local matrix or vector(s) to ps
   */

  def update(delta: Matrix): Unit = matrixClient.update(delta)

  def update(rowId: Int, row: Vector): Unit = matrixClient.update(rowId, row)

  def update(row: Vector): Unit = matrixClient.update(row)

  def update(rowIds: Array[Int], rows: Array[Vector]): Unit = matrixClient.update(rowIds, rows)


  /*
  reset ps matrix or vector(s) and then update by local matrix or vector(s)
   */

  def push(matrix: Matrix): Unit = {
    require(rows == matrix.getNumRows, "matrix dimension does not match!")
    reset()
    update(matrix)
  }

  def push(vector: Vector): Unit = push(vector.getRowId, vector)

  def push(rowId: Int, vector: Vector): Unit = {
    assertRowIndexValid(rowId)
    reset(rowId)
    update(rowId, vector)
  }

  def push(rowIds: Array[Int], rows: Array[Vector]): Unit = {
    assertRowIndexesValid(rowIds)
    reset(rowIds)
    update(rowIds, rows)
  }

  /*
  reset ps matrix or vector(s)
   */
  def reset(): Unit = matrixClient.zero()

  def reset(rowId: Int): Unit = psfUpdate(new Reset(id, rowId)).get

  def reset(rowIds: Array[Int]): Unit = psfUpdate(new Reset(id, rowIds)).get

  /**
    * get matrixClient
    *
    * @return MatrixClient
    */
  private def matrixClient: MatrixClient = {
    assertValid()
    PSContext.instance()
    MatrixClientFactory.get(id, PSContext.getTaskId())
  }

  /*
  checkers
   */
  private[spark] def assertValid(): Unit = if (deleted) throw new AngelException(s"This Matrix has been destroyed!")

  private[spark] def assertRowIndexValid(rowId: Int): Unit =
    if (rowId < 0 || rowId >= rows)
      throw new AngelException(s"rowId out of bound, 0 <= rowId < $rows required, $rowId given")

  private[spark] def assertCompatible(array: Array[Double]): Unit =
    if (columns != array.length)
      throw new AngelException(s"The target array's dimension does not" + s" match matrix dimension")

  private[spark] def assertRowIndexesValid(rowIds: Array[Int]): Unit = rowIds.foreach(assertRowIndexValid)


  /**
    * Destroy this Matrix.
    * Notice: developers must call `destroy` function to release deserted Matrix in PS, otherwise
    * this matrix will occupy the PS resource all the time.
    */
  def destroy(): Unit = {
    PSContext.instance().destroyMatrix(id)
    this.deleted = true
  }

  /**
    * user defined psf func
    *
    * @param func : GetFunc or UpdateFunc
    */

  @deprecated("use psfGet instead", "2.4.0")
  def aggregate(func: GetFunc): GetResult = psfGet(func)

  def psfGet(func: GetFunc): GetResult = {
    matrixClient.get(func)
  }

  @deprecated("use psfUpdate instead", "2.4.0")
  def update(func: UpdateFunc): Unit = psfUpdate(func)

  def psfUpdate(func: UpdateFunc): Future[VoidResult] = {
    matrixClient.update(func)
  }
}

object PSMatrix {
  def dense(rows: Int, cols: Long): DensePSMatrix = {
    LogFactory.getLog(this.getClass).warn(s"rowType not specified, use ${RowType.T_DOUBLE_DENSE} as default")
    dense(rows, cols, RowType.T_DOUBLE_DENSE)
  }

  def dense(rows: Int, cols: Long, rowType: RowType): DensePSMatrix = {
    dense(rows, cols, -1, -1, rowType)
  }

  def dense(rows: Int, cols: Long, rowInBlock: Int, colInBlock: Int, rowType: RowType): DensePSMatrix = {
    require(rowType.isDense, s"Dense towType required, $rowType provided")
    val matrixMeta = PSContext.instance().createDenseMatrix(rows, cols, rowInBlock, colInBlock, rowType)
    new DensePSMatrix(matrixMeta.getId, rows, cols)
  }

  def sparse(rows: Int, cols: Long): SparsePSMatrix = {
    LogFactory.getLog(this.getClass).warn(s"rowType not specified, use ${RowType.T_DOUBLE_SPARSE_LONGKEY} as default")
    sparse(rows, cols, cols, RowType.T_DOUBLE_SPARSE_LONGKEY)
  }

  def sparse(rows: Int, cols: Long, range: Long, rowType: RowType): SparsePSMatrix = {
    require(rowType.isSparse, s"Dense towType required, $rowType provided")
    val matrixMeta = PSContext.instance().createSparseMatrix(rows, cols, range, -1, -1, rowType)
    new SparsePSMatrix(matrixMeta.getId, rows, cols, rowType)
  }

  def zero(rows: Int, cols: Long): DensePSMatrix = {
    PSMatrix.dense(rows, cols)
  }

  /**
    * create dense ps matrix with random elements ranging from low until high
    *
    * @param rows    num of rows
    * @param cols    num of cols
    * @param rowType type of row, require dense row type
    * @param low     lower bound(included)
    * @param high    higher bound(excluded)
    * @return dense ps matrix
    */
  def rand(rows: Int, cols: Long, rowType: RowType = RowType.T_DOUBLE_DENSE,
           low: Double = 0.0, high: Double = 1.0): DensePSMatrix = {
    val mat = PSMatrix.dense(rows, cols)
    PSClient.instance().matrixOps.random(mat)
    mat
  }

  /**
    * Create identity matrix
    */
  def eye(dim: Int): DensePSMatrix = {
    val mat = PSMatrix.dense(dim, dim)
    PSClient.instance().matrixOps.eye(mat)
    mat
  }

  /**
    * Create diagonal matrix
    */
  def diag(array: Array[Double]): DensePSMatrix = {
    val dim = array.length
    val mat = PSMatrix.dense(dim, dim)
    PSClient.instance().matrixOps.diag(mat, array)
    mat
  }

  /**
    * Create a matrix filled with `x`
    */
  def fill(rows: Int, cols: Long, x: Double, rowType: RowType = RowType.T_DOUBLE_DENSE): PSMatrix = {
    assert(rowType.isDense, "fill a sparse matrix is not supported")
    val mat = PSMatrix.dense(rows, cols, rowType)
    PSClient.instance().matrixOps.fill(mat, x)
    mat
  }
}
