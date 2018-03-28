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
import com.tencent.angel.spark.linalg.{DenseVector, SparseVector}

class DensePSVector(override val poolId: Int,
                    override val id: Int,
                    override val dimension: Long) extends ConcretePSVector {

  override def pull: DenseVector = {
    psClient.denseRowOps.pull(this)
  }

  def pull(indices: Array[Long]): SparseVector = {
    psClient.denseRowOps.pull(this, indices)
  }

  def one(): DensePSVector = {
    fill(1.0)
  }

  def zero(): DensePSVector = {
    fill(0.0)
  }

  def fill(value: Double): DensePSVector = {
    psClient.vectorOps.fill(this, value)
    this
  }

  def push(local: DenseVector): DensePSVector = push(local.values)

  def push(local: Array[Double]): DensePSVector = {
    psClient.denseRowOps.push(this, local)
    this
  }

  def randomUniform(min: Double, max: Double): DensePSVector = {
    psClient.denseRowOps.randomUniform(this, min, max)
    this
  }

  def randomNormal(mean: Double, stddev: Double): DensePSVector = {
    psClient.denseRowOps.randomNormal(this, mean, stddev)
    this
  }
}

object DensePSVector {
  def apply(dimension: Int, capacity:Int = 20): DensePSVector = {
    PSContext.instance().createVector(dimension, VectorType.DENSE, capacity, dimension)
      .asInstanceOf[DensePSVector]
  }
}