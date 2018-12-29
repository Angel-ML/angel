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
package com.tencent.angel.spark.models.impl

import java.util.concurrent.Future

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, GetResult}
import com.tencent.angel.ml.matrix.psf.update.base.{UpdateFunc, VoidResult}
import com.tencent.angel.ml.matrix.psf.update.{Fill, Reset}
import com.tencent.angel.psagent.matrix.{MatrixClient, MatrixClientFactory}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.PSMatrix
import com.tencent.angel.spark.util.PSMatrixUtils

class PSMatrixImpl(
    override val id: Int,
    override val rows: Int,
    override val columns: Long,
    override val rowType: RowType)
  extends PSMatrix {

  override def toString: String = {
    s"PSMatrix(id=$id rows=$rows cols=$columns)"
  }

  override def pull(): Matrix = {
    val rowIds = Array.range(0, rows)
    val rowArr = matrixClient.getRows(rowIds, true)
    PSMatrixUtils.createFromVectorArray(id, rowType, rowArr)
  }

  override def pull(rowIds: Array[Int], indexes: Array[Long]): Array[Vector] = {
    require(rowType.isLongKey, s"rowType=$rowType, use `pull(rowIds: Array[Int], indexes: Array[Int])` instead")
    matrixClient.get(rowIds, indexes)
  }

  override def pull(rowIds: Array[Int], indexes: Array[Int]): Array[Vector] = {
    require(rowType.isIntKey, s"rowType=$rowType, use `pull(rowIds: Array[Int], indexes: Array[Long])` instead")
    matrixClient.get(rowIds, indexes)
  }

  override def pull(rowId: Int, indexes: Array[Long]): Vector = {
    require(rowType.isLongKey, s"rowType=$rowType, use `pull(rowIds: Int, indexes: Array[Int])` instead")
    matrixClient.get(rowId, indexes)
  }

  override def pull(rowId: Int, indexes: Array[Int]): Vector = {
    require(rowType.isIntKey, s"rowType=$rowType, use `pull(rowIds: Int, indexes: Array[Long])` instead")
    matrixClient.get(rowId, indexes)
  }

  override def pull(rowIds: Array[Int], batchSize: Int = -1): Array[Vector] = {
    require(rowIds.forall(rowId => rowId >= 0 && rowId < rows), "rowId out of range")
    if (batchSize <= 0)
      matrixClient.getRows(rowIds, true)
    else
      matrixClient.getRows(rowIds, batchSize, true)
  }

  override def pull(rowId: Int): Vector = matrixClient.getRow(rowId, true)


  /*
  increment local matrix or vector(s) to ps
   */
  def increment(delta: Matrix): Unit = {
    matrixClient.increment(delta, true)
  }

  def increment(delta: Vector): Unit = {
    require(rowType.compatible(delta.getType), s"can't increment $rowType by ${delta.getType}")
    matrixClient.increment(delta, true)
  }

  def increment(rowId: Int, delta: Vector): Unit = {
    require(rowType.compatible(delta.getType), s"can't increment $rowType by ${delta.getType}")
    matrixClient.increment(rowId, delta, true)
  }

  def increment(rowIds: Array[Int], deltas: Array[Vector]): Unit ={
    require(deltas.forall(_.getType.compatible(rowType)),
      s"can't increment $rowType by ${deltas.map(_.getType).mkString(",")}")
    matrixClient.increment(rowIds, deltas, true)
  }

  /**
   * get matrixClient
   *
   * @return MatrixClient
   */
  private def matrixClient: MatrixClient = {
    assertValid()
    PSContext.instance()
    MatrixClientFactory.get(id, PSContext.getTaskId)
  }

  /*
 checkers
  */
  private[spark] def assertValid(): Unit = if (deleted) throw new AngelException(s"This Matrix has been destroyed!")

  def update(delta: Matrix): Unit = matrixClient.update(delta)

  def update(rowId: Int, row: Vector): Unit = {
    require(rowType.compatible(row.getType), s"can't update $rowType by ${row.getType}")
    matrixClient.update(rowId, row)
  }

  def update(row: Vector): Unit = {
    require(rowType.compatible(row.getType), s"can't update $rowType by ${row.getType}")
    matrixClient.update(row)
  }

  def update(rowIds: Array[Int], rows: Array[Vector]): Unit = {
    require(rows.forall(_.getType.compatible(rowType)),
      s"can't update $rowType by ${rows.map(_.getType).mkString(",")}")
    matrixClient.update(rowIds, rows)
  }

  /*
 update local matrix or vector(s) to ps
  */
  def push(matrix: Matrix): Unit = {
    require(rows == matrix.getNumRows, "matrix dimension does not match!")
    reset()
    update(matrix)
  }

  def push(vector: Vector): Unit = {
    push(vector.getRowId, vector)
  }

  def push(rowId: Int, vector: Vector): Unit = {
    require(rowType.compatible(vector.getType), s"can't push $rowType by ${vector.getType}")
    assertRowIndexValid(rowId)
    reset(rowId)
    update(rowId, vector)
  }

  def push(rowIds: Array[Int], rows: Array[Vector]): Unit = {
    require(rows.forall(_.getType.compatible(rowType)), s"can't push $rowType by ${rows.map(_.getType).mkString(",")}")
    assertRowIndexesValid(rowIds)
    reset(rowIds)
    update(rowIds, rows)
  }

  /*
 reset ps matrix or vector(s)
  */
  def reset(): Unit = matrixClient.zero()

  def reset(rowId: Int): Unit = {
    assertRowIndexValid(rowId)
    psfUpdate(new Reset(id, rowId)).get
  }

  def reset(rowIds: Array[Int]): Unit = {
    assertRowIndexesValid(rowIds)
    psfUpdate(new Reset(id, rowIds)).get
  }

  /**
   * fill `rows` with `values`
   */
  def fill(rows: Array[Int], values: Array[Double]): PSMatrix = {
    require(rowType.isDense, "fill a sparse matrix is not supported")
    this.psfUpdate(new Fill(id, rows, values)).get
    this
  }

  def psfUpdate(func: UpdateFunc): Future[VoidResult] = {
    matrixClient.update(func)
  }

  def psfGet(func: GetFunc): GetResult = {
    matrixClient.get(func)
  }

  /**
   * Destroy this Matrix.
   * Notice: developers must call `destroy` function to release deserted Matrix in PS, otherwise
   * this matrix will occupy the PS resource all the time.
   */
  override def destroy(): Unit = {
    PSContext.instance().destroyMatrix(id)
    this.deleted = true
  }

  private[spark] def assertRowIndexValid(rowId: Int): Unit ={
    require(rowId >= 0 && rowId < rows, s"rowId out of bound, 0 <= rowId < $rows required, $rowId given")
  }

  private[spark] def assertRowIndexesValid(rowIds: Array[Int]): Unit = rowIds.foreach(assertRowIndexValid)

}
