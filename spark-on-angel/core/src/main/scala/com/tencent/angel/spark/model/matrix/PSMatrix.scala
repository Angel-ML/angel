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

import org.apache.spark.SparkException

import com.tencent.angel.ml.matrix.MatrixMeta
import com.tencent.angel.ml.matrix.psf.update.enhance.map.MapFunc
import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.model.PSModelProxy

abstract class PSMatrix extends Serializable {
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
  def push(rowId: Int, array: Array[Double]): Unit = {
    PSClient().push(toProxy(rowId), array)
  }

  /**
   * Pull a row to local from PS
   */
  def pull(rowId: Int): Array[Double] = {
    PSClient().pull(toProxy(rowId))
  }

  /**
   * Increment a row of matrix with `array`
   */
  def increment(rowId: Int, array: Array[Double]): Unit = {
    PSClient().increment(toProxy(rowId), array)
  }

  /**
   * Update a row of matrix with `func`
   */
  def update(rowId: Int, func: MapFunc): Unit = {
    PSClient().mapInPlace(toProxy(rowId), func)
  }

  /**
   * Destroy this Matrix.
   * Notice: developers must call `destroy` function to release deserted Matrix in PS, otherwise
   * this matrix will occupy the PS resource all the time.
   */
  def destroy(): Unit = {
    val psContext = PSContext.getOrCreate()
    psContext.destroyMatrix(this.meta)
    this.deleted = true
  }

  /**
   * Create a Proxy with `rowId`
   */
  private[spark] def toProxy(rowId: Int): PSModelProxy = {
    new PSModelProxy(meta.getId, rowId, meta.getColNum)
  }

  private[spark] def assertValid(): Unit = {
    if (deleted) {
      throw new SparkException(s"This Matrix has been destroyed!")
    }
  }

  private[spark] def assertCompatible(array: Array[Double]): Unit = {
    if (meta.getColNum != array.length) {
      throw new SparkException(s"The target array's dimension does not" +
        s" match matrix dimension")
    }
  }
}

object PSMatrix {
  def dense(rows: Int, cols: Int): DenseMatrix = DenseMatrix(rows, cols)
  def sparse(rows: Int, cols: Int): SparseMatrix = SparseMatrix(rows, cols)
}

object MatrixType extends Enumeration {
  type MatrixType = Value
  val DENSE, SPARSE = Value
}
