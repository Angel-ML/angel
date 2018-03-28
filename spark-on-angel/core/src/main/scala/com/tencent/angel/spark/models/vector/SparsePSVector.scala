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
 *
 */

package com.tencent.angel.spark.models.vector

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.linalg.SparseVector


class SparsePSVector(override val poolId: Int,
                     override val id: Int,
                     override val dimension: Long) extends ConcretePSVector {

  override def pull: SparseVector = {
    psClient.sparseRowOps.pull(this)
  }

  def push(local: SparseVector): SparsePSVector = {
    psClient.sparseRowOps.push(this, local)
    this
  }

  def increment(delta: SparseVector): Unit = {
    psClient.sparseRowOps.increment(this, delta)
  }

  def pull(indices: Array[Long]): SparseVector = {
    psClient.sparseRowOps.pull(this, indices)
  }

  def zero(): Unit = fill(0.0)

  def fill(value: Double): Unit = {
    psClient.vectorOps.fill(this, value)
  }

  /**
   * Sparse PSVector is stored as Long2DoubleOpenHashMap in PS. `compress` is try to remove the
   * Map.Entry[Long, Double] which value is equal to Long2DoubleOpenHashMap.defaultReturnValue,
   * this will reduce the store size of Sparse PSVector in PS.
   */
  def compress(): Unit = {
    psClient.sparseRowOps.compress(this)
  }
}

object SparsePSVector{

  def apply(dimension: Long, capacity: Int): SparsePSVector = {
    PSContext.instance().createVector(dimension, VectorType.SPARSE, capacity, dimension)
      .asInstanceOf[SparsePSVector]
  }

  def apply(dimension: Long, capacity: Int, range: Long): SparsePSVector = {
    PSContext.instance().createVector(dimension, VectorType.SPARSE, capacity, range)
      .asInstanceOf[SparsePSVector]
  }
}