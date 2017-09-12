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

package com.tencent.angel.spark.math.vector.decorator

import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.math.vector.PSVector
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkEnv, TaskContext}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

/**
 * RemotePSVector implements a set of operations between PSVector and local double array.
 */
private[spark] class RemotePSVector(component:PSVector) extends PSVectorDecorator(component) {

  import RemotePSVector._
  import MergeType._


  def pull(fromCache: Boolean = false): Array[Double] = {
    if (fromCache) {
      if (!localCache.contains(this)) {
        localCache.synchronized {
          if (!localCache.contains(this)) {
            val localArray = PSClient.instance().pull(this)
            localCache.put(this, localArray)
          }
        }
      }
    } else {
      val localArray = PSClient.instance().pull(this)
      if (localCache.contains(this)) {
        localCache(this) = localArray
      } else {
        localCache.put(this, localArray)
      }
    }
    localCache(this)
  }

  /**
   * Push local array to PSVector in PS nodes
   */
  def push(value: Array[Double]): Unit = {
    PSClient.instance().push(this, value)
  }

  /**
   * Increment a local dense Double array to PSVector
   * Notice: You should call `flushOne` to flushOne `mergedArray` result to PS nodes.
   */
  def increment(delta: Array[Double]): Unit = {
    mergeCache.synchronized {
      init(INCREMENT, this)
      val mergedArray = mergeCache.get(this).get._2
      PSClient.instance().BLAS.daxpy(this.dimension, 1.0, delta, 1, mergedArray, 1)
    }
  }

  /**
   * Increment a local sparse Double array to PSVector
   * Notice: You should call `flushOne` to flushOne `mergedArray` result to PS nodes.
   */
  def increment(indices: Array[Int], values: Array[Double]): Unit = {
    require(indices.length == values.length && indices.length <= this.dimension)

    mergeCache.synchronized {
      init(INCREMENT, this)
      val mergedArray = mergeCache.get(this).get._2
      var i = 0
      while (i < indices.length) {
        mergedArray(indices(i)) += values(i)
        i += 1
      }
    }
  }

  /**
   * Increment a local dense Double array to PSVector, and flushOne to PS nodes immediately
   */
  def incrementAndFlush(delta: Array[Double]): Unit = {
    PSClient.instance().increment(this, delta)
  }

  // =======================================================================
  // Merge Operator
  // =======================================================================

  /**
   * Find the maximum number of each dimension for PSVector and local dense array.
   * Notice: You should call `flushOne` to flushOne `mergedArray` result to PS nodes.
   */
  def mergeMax(other: Array[Double]): Unit = {
    val dim = this.dimension
    require(other.length == dim)
    mergeCache.synchronized {
      init(MAX, this)
      val mergedArray = mergeCache.get(this).get._2
      var i = 0
      while (i < dim) {
        mergedArray(i) = math.max(mergedArray(i), other(i))
        i += 1
      }
    }
  }

  /**
   * Find the maximum number of each dimension for PSVector and local sparse array.
   * Notice: You should call `flushOne` to flushOne `mergedArray` result to PS nodes.
   */
  def mergeMax(indices: Array[Int], values: Array[Double]): Unit = {
    val dim = this.dimension
    require(indices.length == values.length && indices.length <= dim)

    mergeCache.synchronized {
      init(MAX, this)

      val mergedArray = mergeCache.get(this).get._2
      var i = 0
      while (i < indices.length) {
        val index = indices(i)
        mergedArray(index) = math.max(mergedArray(index), values(i))
        i += 1
      }
    }
  }

  /**
   * Find the maximum number of each dimension for PSVector and local sparse array, and flushOne to
   * PS nodes immediately
   */
  def mergeMaxAndFlush(other: Array[Double]): Unit = {
    PSClient.instance().mergeMax(this, other)
  }

  /**
   * Find the maximum number of each dimension for PSVector and local dense array.
   * Notice: You should call `flushOne` to flushOne `mergedArray` result to PS nodes.
   */
  def mergeMin(other: Array[Double]): Unit = {
    require(other.length == this.dimension)
    mergeCache.synchronized {
      init(MIN, this)
      val mergedArray = mergeCache.get(this).get._2
      var i = 0
      while (i < mergedArray.length) {
        mergedArray(i) = math.min(mergedArray(i), other(i))
        i += 1
      }
    }
  }

  /**
   * Find the minimum number of each dimension for PSVector and local sparse array.
   * Notice: You should call `flushOne` to flushOne `mergedArray` result to PS nodes.
   */
  def mergeMin(indices: Array[Int], values: Array[Double]): Unit = {
    val dim = this.dimension
    require(indices.length == values.length && indices.length <= dim)
    mergeCache.synchronized {
      init(MIN, this)
      val mergedArray = mergeCache.get(this).get._2
      var i = 0
      while (i < indices.length) {
        val index = indices(i)
        mergedArray(index) = math.min(mergedArray(index), values(i))
        i += 1
      }
    }
  }

  /**
   * Find the minimum number of each dimension for PSVector and local dense array.
   * Notice: You should call `flushOne` to flushOne `mergedArray` result to PS nodes.
   */
  def mergeMinAndFlush(other: Array[Double]): Unit = {
    PSClient.instance().mergeMin(this, other)
  }

  def flush(): Unit = {
    if (TaskContext.get() == null) { // run flushOne on driver
      val sparkConf = SparkEnv.get.conf
      val executorNum = sparkConf.getInt("spark.executor.instances", 1)
      val core = sparkConf.getInt("spark.executor.cores", 1)
      val totalTask = core * executorNum
      val spark = SparkSession.builder().getOrCreate()
      spark.sparkContext.range(0, totalTask, 1, totalTask)
        .foreach { taskId =>
          RemotePSVector.flushOne(this)
        }
    } else { // run flushOne on executor
      RemotePSVector.flushOne(this)
    }
  }

}

object RemotePSVector {
  import MergeType._

  val localCache = new mutable.WeakHashMap[RemotePSVector, Array[Double]]
  val mergeCache = new TrieMap[RemotePSVector, (MergeType, Array[Double])]()

  val keyString = localCache.keys.map(key => key.poolId + "_" + key.id).mkString(",")

  private def init(mergeType: MergeType, vector: RemotePSVector) = {
    if (mergeCache.contains(vector)) {
      require(mergeCache.get(vector).get._1 == mergeType, "Do not use different merge methods on the same RemotePSVector!")
    }

    mergeCache.putIfAbsent(vector, {
      val initArray = mergeType match {
        case INCREMENT => Array.fill(vector.dimension)(0.0)
        case MAX => Array.fill(vector.dimension)(Double.MinValue)
        case MIN => Array.fill(vector.dimension)(Double.MaxValue)
        case UNDEFINED => throw new Exception("undefined merge type")
      }
      (mergeType, initArray)
    })
  }

  private def flushOne(proxy: RemotePSVector)  = {
    mergeCache.synchronized {
      if (mergeCache.contains(proxy)) {
        val (mergeType, mergedArray) = mergeCache.get(proxy).get
        mergeType match {
          case INCREMENT => PSClient.instance().increment(proxy, mergedArray)
          case MAX => PSClient.instance().mergeMax(proxy, mergedArray)
          case MIN => PSClient.instance().mergeMin(proxy, mergedArray)
        }
        mergeCache.remove(proxy)
      }
    }
  }

  private[spark] def flushAll() = {
    if (TaskContext.get() == null) { // run flush on driver
      val sparkConf = SparkEnv.get.conf
      val executorNum = sparkConf.getInt("spark.executor.instances", 1)
      val core = sparkConf.getInt("spark.executor.cores", 1)
      val totalTask = core * executorNum
      val spark = SparkSession.builder().getOrCreate()
      spark.sparkContext.range(0, totalTask, 1, totalTask)
        .foreach { taskId =>
          for (key <- mergeCache.keys) {
            RemotePSVector.flushOne(key)
          }
        }
    } else { // run flushOne on executor
      for (key <- mergeCache.keys) {
        RemotePSVector.flushOne(key)
      }
    }
  }
}

object MergeType extends Enumeration {
  type MergeType = Value
  val UNDEFINED, INCREMENT, MAX, MIN = Value
}
