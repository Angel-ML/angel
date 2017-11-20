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


class SparsePSVector(override val poolId: Int,
                     override val id: Int,
                     override val dimension: Long) extends PSVector {

  def fill(values: Array[(Long, Double)]): Unit = {
    psClient.sparseRowOps.push(this, values)
  }

  def increment(delta: Array[(Long, Double)]): Unit = {
    psClient.sparseRowOps.increment(this, delta)
  }

  def pull(indices: Array[Long]): Array[(Long, Double)] = {
    psClient.sparseRowOps.pull(this, indices)
  }

  def sparsePull(): Array[(Long, Double)] = {
    psClient.sparseRowOps.pull(this)
  }

  // other method of SparsePSVector is going to launch
}

object SparsePSVector{
  def apply(dimension: Long, capacity:Int = 20): SparsePSVector = {
    PSContext.instance().createVector(dimension, VectorType.SPARSE, capacity).asInstanceOf[SparsePSVector]
  }
}