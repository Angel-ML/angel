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

import org.apache.spark.SparkException

import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.models.PSModelProxy

/**
 * RemotePSVector implements a set of operations between PSVector and local double array.
 */
private[spark] class RemotePSVector (override val proxy: PSModelProxy) extends PSVector {
  import MergeType._

  def toBreeze: BreezePSVector = new BreezePSVector(proxy)

  /**
   * Pull PSVector from PS nodes to local.
   * @return
   */
  def pull(): Array[Double] = {
    if (localArray == null) {
      this.synchronized {
        if (localArray == null) {
          localArray = PSClient().pull(proxy)
        }
      }
    }
    localArray
  }

  /**
   * Push local array to PSVector in PS nodes
   */
  def push(value: Array[Double]): Unit = {
    PSClient().push(proxy, value)
  }

  /**
   * Increment a local dense Double array to PSVector
   * Notice: You should call `flush` to flush `mergedArray` result to PS nodes.
   */
  def increment(delta: Array[Double]): Unit = {
    init(INCREMENT)
    require(delta.length == mergedArray.length)
    val n = mergedArray.length
    PSClient().BLAS.daxpy(n, 1.0, delta, 1, mergedArray, 1)
  }

  /**
   * Increment a local sparse Double array to PSVector
   * Notice: You should call `flush` to flush `mergedArray` result to PS nodes.
   */
  def increment(indices: Array[Int], values: Array[Double]): Unit = {
    init(INCREMENT)
    require(indices.length == values.length && indices.length <= mergedArray.length)
    var i = 0
    while (i < indices.length) {
      mergedArray(indices(i)) += values(i)
      i += 1
    }
  }

  /**
   * Increment a local dense Double array to PSVector, and flush to PS nodes immediately
   */
  def incrementAndFlush(delta: Array[Double]): Unit = {
    PSClient().increment(proxy, delta)
  }

  /**
   * Find the maximum number of each dimension for PSVector and local dense array.
   * Notice: You should call `flush` to flush `mergedArray` result to PS nodes.
   */
  def mergeMax(other: Array[Double]): Unit = {
    init(MAX)
    require(other.length == mergedArray.length)
    var i = 0
    while (i < mergedArray.length) {
      mergedArray(i) = math.max(mergedArray(i), other(i))
      i += 1
    }
  }

  /**
   * Find the maximum number of each dimension for PSVector and local sparse array.
   * Notice: You should call `flush` to flush `mergedArray` result to PS nodes.
   */
  def mergeMax(indices: Array[Int], values: Array[Double]): Unit = {
    init(MAX)
    require(indices.length == values.length && indices.length <= mergedArray.length)
    var i = 0
    while (i < indices.length) {
      val index = indices(i)
      mergedArray(index) = math.max(index, values(i))
      i += 1
    }
  }

  /**
   * Find the maximum number of each dimension for PSVector and local sparse array, and flush to
   * PS nodes immediately
   */
  def mergeMaxAndFlush(other: Array[Double]): Unit = {
    PSClient().mergeMax(proxy, other)
  }

  /**
   * Find the maximum number of each dimension for PSVector and local dense array.
   * Notice: You should call `flush` to flush `mergedArray` result to PS nodes.
   */
  def mergeMin(other: Array[Double]): Unit = {
    init(MIN)
    require(other.length == mergedArray.length)
    var i = 0
    while (i < mergedArray.length) {
      mergedArray(i) = math.min(mergedArray(i), other(i))
      i += 1
    }
  }

  /**
   * Find the minimum number of each dimension for PSVector and local sparse array.
   * Notice: You should call `flush` to flush `mergedArray` result to PS nodes.
   */
  def mergeMin(indices: Array[Int], values: Array[Double]): Unit = {
    init(MIN)
    require(indices.length == values.length && indices.length <= mergedArray.length)
    var i = 0
    while (i < indices.length) {
      val index = indices(i)
      mergedArray(index) = math.min(index, values(i))
      i += 1
    }
  }

  /**
   * Find the minimum number of each dimension for PSVector and local dense array.
   * Notice: You should call `flush` to flush `mergedArray` result to PS nodes.
   */
  def mergeMinAndFlush(other: Array[Double]): Unit = {
    PSClient().mergeMin(proxy, other)
  }


  @transient @volatile private var localArray: Array[Double] = _

  @transient private var mergeType: MergeType = UNDEFINED

  @transient private var mergedArray: Array[Double] = _

  private def init(mergeType: MergeType): Unit = {
    if (mergedArray == null) {
      mergedArray = new Array[Double](proxy.numDimensions)
      this.mergeType = mergeType
      PSClient().register(this)
    } else {
      if (this.mergeType != mergeType) {
        throw new SparkException(
          "Do not use different merge methods on the same RemotePSVector!")
      }
    }
  }

  private[spark] def flush(): Unit = {
    if (mergedArray != null) {
      mergeType match {
        case INCREMENT =>
          PSClient().increment(proxy, mergedArray)
        case MAX =>
          PSClient().mergeMax(proxy, mergedArray)
        case MIN =>
          PSClient().mergeMin(proxy, mergedArray)
      }
      mergeType = UNDEFINED
      mergedArray = null
    }
  }

}

object MergeType extends Enumeration {
  type MergeType = Value
  val UNDEFINED, INCREMENT, MAX, MIN = Value
}
