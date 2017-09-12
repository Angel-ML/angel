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

package com.tencent.angel.spark.math.matrix

import com.tencent.angel.ml.matrix.MatrixMeta
import com.tencent.angel.ml.matrix.psf.update.enhance.map.MapFunc
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.math.vector.{DensePSVector, PSVector}
import org.apache.spark.SparkException

import com.tencent.angel.spark.client.PSClient

abstract class PSMatrix {
  val rows: Int
  val columns: Int
  val meta: MatrixMeta

  private var deleted: Boolean = false

  def size: Int = rows * columns

  /**
   * Operations for each row of `PSMatrix`
   */

  /**
   * Push a array to `rowId` in matrix
   */
  def push(rowId: Int, array: Array[Double]) = {
    PSClient.instance().push(getRow(rowId), array)
  }

  /**
   * Pull a row to local from PS
   */
  def pull(rowId: Int): Array[Double] = {
    PSClient.instance().pull(getRow(rowId))
  }

  /**
   * Increment a row of matrix with `array`
   */
  def increment(rowId: Int, array: Array[Double]) = {
    PSClient.instance().increment(getRow(rowId), array)
  }

  /**
   * Update a row of matrix with `func`
   */
  def update(rowId: Int, func: MapFunc) = {
    PSClient.instance().mapInPlace(getRow(rowId), func)
  }

  /**
   * Destroy this Matrix.
   * Notice: developers must call `destroy` function to release deserted Matrix in PS, otherwise
   * this matrix will occupy the PS resource all the time.
   */
  def destroy() = {
    PSContext.instance().destroyMatrix(this.meta)
    this.deleted = true
  }

  private[spark] def getRow(rowId: Int): PSVector = {
    new DensePSVector(meta.getId, rowId, meta.getRowNum())
  }

  private[spark] def assertValid() = {
    if (deleted) {
      throw new SparkException(s"This Matrix has been destroyed!")
    }
  }

  private[spark] def assertCompatible(array: Array[Double]) = {
    if (meta.getColNum != array.length) {
      throw new SparkException(s"The target array's dimension does not" +
        s" match matrix dimension")
    }
  }
}

object PSMatrix {
  def dense(rows: Int, cols: Int): DensePSMatrix = DensePSMatrix(rows, cols)
  def sparse(rows: Int, cols: Int): SparsePSMatrix = SparsePSMatrix(rows, cols)
}

object MatrixType extends Enumeration {
  type MatrixType = Value
  val DENSE, SPARSE = Value
}
