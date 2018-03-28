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

package com.tencent.angel.spark.models.vector.enhanced

import com.tencent.angel.spark.models.vector.{ConcretePSVector, PSVector}
import com.tencent.angel.spark.linalg.{BLAS, DenseVector, SparseVector, Vector}
import com.tencent.angel.spark.models.vector.cache.{Local2RemoteOps, MergeType, PullMan, PushMan}

/**
 * CachedPSVector implements a more efficient Vector, which can benefit multi tasks on one executor
 */

private[spark] class CachedPSVector(component: ConcretePSVector) extends PSVectorDecorator(component) {

  override val dimension = component.dimension
  override val id  = component.id
  override val poolId  = component.poolId

  def pullFromCache(): Vector = {
    PullMan.pullFromCache(this)
  }

  /**
    * Increment a local dense Double array to PSVector
    * Notice: You should call `flushOne` to flushOne `mergedArray` result to PS nodes.
    */
  def incrementWithCache(delta: Vector): Unit = {
    val mergedCache = PushMan.getFromIncrementCache(this)
    mergedCache.synchronized {
      BLAS.axpy(1.0, delta, mergedCache)
    }
  }

  /**
    * Increment a local sparse Double array to PSVector
    * Notice: You should call `flushOne` to flushOne `mergedArray` result to PS nodes.
    */
  def incrementWithCache(indices: Array[Long], values: Array[Double]): Unit = {
    require(indices.length == values.length && indices.length <= this.dimension)
    incrementWithCache(new SparseVector(dimension, indices, values))
  }

  def increment(local: Vector): Unit = {
    this.assertValid()
    require(this.dimension == local.length)
    Local2RemoteOps.increment(this, local)
  }

  // =======================================================================
  // Merge Operator
  // =======================================================================

  /**
   * Find the maximum number of each dimension for PSVector and local dense array.
   * Notice: You should call `flushOne` to flushOne `mergedArray` result to PS nodes.
   */
  def mergeMaxWithCache(other: Array[Double]): Unit = {
    require(other.length == dimension)
    val mergedArray = PushMan.getFromMaxCache(this)
    mergedArray.synchronized {
      mergedArray match {
        case dv: DenseVector =>
          other.indices.foreach { i =>
            if (other(i) > dv.values(i)) {
              dv.values(i) = other(i)
            }
          }
      }
    }
  }

  /**
   * Find the maximum number of each dimension for PSVector and local sparse array, and flushOne to
   * PS nodes immediately
   */
  def mergeMax(other: Vector): Unit = {
    Local2RemoteOps.mergeMax(this, other)
  }

  /**
   * Find the minimum number of each dimension for PSVector and local sparse array.
   * Notice: You should call `flushOne` to flushOne `mergedArray` result to PS nodes.
   */
  def mergeMinWithCache(other: Array[Double]): Unit = {
    require(other.length == dimension)
    val mergedArray = PushMan.getFromMinCache(this)
    mergedArray.synchronized {
      mergedArray match {
        case dv: DenseVector =>
          other.indices.foreach { i =>
            if (other(i) < dv.values(i)) {
              dv.values(i) = other(i)
            }
          }
      }
    }
  }

  /**
   * Find the minimum number of each dimension for PSVector and local dense array.
   * Notice: You should call `flushOne` to flushOne `mergedArray` result to PS nodes.
   */
  def mergeMin(delta: Vector): Unit = {
    Local2RemoteOps.mergeMin(this, delta)
  }

  def flushIncrement(): Unit = PushMan.flush(this.component, MergeType.INCREMENT)
  def flushMax(): Unit = PushMan.flush(this.component, MergeType.MAX)
  def flushMin(): Unit = PushMan.flush(this.component, MergeType.MIN)

  override def delete(): Unit = component.delete()
}

