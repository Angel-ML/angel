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

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.matrix.psf.get.base.{GetFunc, GetResult}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.PSModel

abstract class PSMatrix(
    val id: Int,
    val rows: Int,
    val columns: Long) extends PSModel {

  private var deleted: Boolean = false

  def size: Long = rows * columns

  override def toString: String = {
    s"PSMatrix(id=$id rows=$rows cols=$columns)"
  }

  /**
   * Destroy this Matrix.
   * Notice: developers must call `destroy` function to release deserted Matrix in PS, otherwise
   * this matrix will occupy the PS resource all the time.
   */
  def destroy() = {
    PSContext.instance().destroyMatrix(id)
    this.deleted = true
  }

  def aggregate(func: GetFunc): GetResult = {
    psClient.matrixOps.aggregate(this, func)
  }

  private[spark] def assertValid() = {
    if (deleted) {
      throw new AngelException(s"This Matrix has been destroyed!")
    }
  }

  private[spark] def assertCompatible(array: Array[Double]) = {
    if (columns != array.length) {
      throw new AngelException(s"The target array's dimension does not" +
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
