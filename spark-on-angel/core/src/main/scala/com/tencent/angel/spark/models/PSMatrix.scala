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


package com.tencent.angel.spark.models

import java.util.concurrent.Future

import scala.collection.Map
import org.apache.commons.logging.LogFactory
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, GetResult}
import com.tencent.angel.ml.matrix.psf.update.{Diag, Eye, FullFill, Random}
import com.tencent.angel.ml.matrix.psf.update.base.{UpdateFunc, VoidResult}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.impl.PSMatrixImpl

abstract class PSMatrix extends PSModel {
  val id: Int
  val name: String
  val rows: Int
  val columns: Long
  val rowType: RowType
  private[models] val log = LogFactory.getLog(this.getClass)
  private[models] var deleted: Boolean = false


  /*
  Pull PS matrix or vector(s) to local.
   */

  def pull(): Matrix

  def pull(rowIds: Array[Int], indexes: Array[Long]): Array[Vector]

  def pull(rowIds: Array[Int], indexes: Array[Int]): Array[Vector]

  def pull(rowId: Int, indexes: Array[Long]): Vector

  def pull(rowId: Int, indexes: Array[Int]): Vector

  def pull(rowId: Int): Vector

  def pull(rowIds: Array[Int], batchSize: Int = -1): Array[Vector]

  def asyncPull(rowIds: Array[Int], indexes: Array[Long]): Future[Array[Vector]]

  def asyncPull(rowIds: Array[Int], indexes: Array[Int]): Future[Array[Vector]]

  def asyncPull(rowId: Int, indexes: Array[Long]): Future[Vector]

  def asyncPull(rowId: Int, indexes: Array[Int]): Future[Vector]

  def asyncPull(rowId: Int): Future[Vector]


  /*
   increment local matrix or vector(s) to ps
    */

  def increment(delta: Matrix)

  def increment(delta: Vector)

  def increment(rowId: Int, delta: Vector)

  def increment(rowIds: Array[Int], deltas: Array[Vector])

  def asyncIncrement(delta: Matrix): Future[VoidResult]

  def asyncIncrement(delta: Vector): Future[VoidResult]

  def asyncIncrement(rowId: Int, delta: Vector): Future[VoidResult]

  def asyncIncrement(rowIds: Array[Int], deltas: Array[Vector]): Future[VoidResult]

  /*
  update local matrix or vector(s) to ps
   */

  def update(delta: Matrix)

  def update(rowId: Int, row: Vector)

  def update(row: Vector)

  def update(rowIds: Array[Int], rows: Array[Vector])

  def asyncUpdate(delta: Matrix): Future[VoidResult]

  def asyncUpdate(rowId: Int, row: Vector): Future[VoidResult]

  def asyncUpdate(row: Vector): Future[VoidResult]

  def asyncUpdate(rowIds: Array[Int], rows: Array[Vector]): Future[VoidResult]


  /*
  reset ps matrix or vector(s) and then update by local matrix or vector(s)
   */

  def push(matrix: Matrix)

  def push(vector: Vector)

  def push(rowId: Int, vector: Vector)

  def push(rowIds: Array[Int], rows: Array[Vector])

  def asyncPush(matrix: Matrix): Future[VoidResult]

  def asyncPush(vector: Vector): Future[VoidResult]

  def asyncPush(rowId: Int, vector: Vector): Future[VoidResult]

  def asyncPush(rowIds: Array[Int], rows: Array[Vector]): Future[VoidResult]

  /*
  reset ps matrix or vector(s)
   */
  def reset()

  def reset(rowId: Int)

  def reset(rowIds: Array[Int])

  def asyncReset(): Future[VoidResult]

  def asyncReset(rowId: Int): Future[VoidResult]

  def asyncReset(rowIds: Array[Int]): Future[VoidResult]

  /**
   * fill `rows` with `values`
   */
  def fill(rows: Array[Int], values: Array[Double]): PSMatrix

  def psfGet(func: GetFunc): GetResult

  def asyncPsfGet(func: GetFunc): Future[GetResult]

  def psfUpdate(func: UpdateFunc): Future[VoidResult]

  def asyncPsfUpdate(func: UpdateFunc): Future[VoidResult]

  def checkpoint(epochId:Int = 0)

  def destroy()
}

object PSMatrix{
  def sparse(rows: Int, cols: Long): PSMatrix = {
    sparse(rows, cols, cols, RowType.T_DOUBLE_SPARSE_LONGKEY)
  }

  def sparse(rows: Int, cols: Long, range: Long, rowType: RowType,
      additionalConfiguration:Map[String, String] = Map()): PSMatrix = {
    require(rowType.isSparse, s"Sparse rowType required, $rowType provided")
    val matrixMeta = PSContext.instance()
      .createSparseMatrix(rows, cols, range, -1, -1, rowType, additionalConfiguration)
    new PSMatrixImpl(matrixMeta.getId, matrixMeta.getName, rows, cols, rowType)
  }

  @deprecated("use dense directly", "2.0.0")
  def zero(rows: Int, cols: Long): PSMatrix = {
    dense(rows, cols)
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
      low: Double = 0.0, high: Double = 1.0): PSMatrix = {
    val mat = dense(rows, cols, rowType)
    mat.psfUpdate(new Random(mat.id, low, high)).get()
    mat
  }

  /**
   * Create identity matrix
   */
  def eye(dim: Int, rowType: RowType = RowType.T_DOUBLE_DENSE): PSMatrix = {
    assert(rowType.isDense, s"eye matrix with RowType $rowType is not supported yet")
    val mat = dense(dim, dim)
    mat.psfUpdate(new Eye(mat.id)).get()
    mat
  }

  /**
   * Create diagonal matrix
   */
  def diag(array: Array[Double], rowType: RowType = RowType.T_DOUBLE_DENSE): PSMatrix = {
    assert(rowType.isDense, s"diagonal matrix with RowType $rowType is not supported yet")
    val dim = array.length
    val mat = dense(dim, dim)
    mat.psfUpdate(new Diag(mat.id, array)).get()
    mat
  }

  def dense(rows: Int, cols: Long): PSMatrix = {
    dense(rows, cols, RowType.T_DOUBLE_DENSE)
  }

  def dense(rows: Int, cols: Long, rowType: RowType): PSMatrix = {
    dense(rows, cols, -1, -1, rowType)
  }

  def dense(rows: Int, cols: Long, rowInBlock: Int, colInBlock: Int, rowType: RowType,
      additionalConfiguration: Map[String, String] = Map()): PSMatrix = {
    require(rowType.isDense, s"Dense towType required, $rowType provided")
    val matrixMeta = PSContext.instance()
      .createDenseMatrix(rows, cols, rowInBlock, colInBlock, rowType, additionalConfiguration)
    new PSMatrixImpl(matrixMeta.getId, matrixMeta.getName, rows, cols, rowType)
  }

  def matrix(mc : MatrixContext): PSMatrix = {
    val matrixMeta = PSContext.instance()
      .createMatrix(mc)
    new PSMatrixImpl(matrixMeta.getId, matrixMeta.getName, matrixMeta.getRowNum, matrixMeta.getColNum, matrixMeta.getRowType)
  }

  /**
   * Create a matrix filled with `x`
   */
  def fill(rows: Int, cols: Long, x: Double, rowType: RowType = RowType.T_DOUBLE_DENSE): PSMatrix = {
    assert(rowType.isDense, "fill a sparse matrix is not supported")
    val mat = dense(rows, cols, rowType)
    mat.psfUpdate(new FullFill(mat.id, x)).get()
    mat
  }
}