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

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkEnv, TaskContext}

import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.models.PSModelProxy

/**
 * RemotePSVector implements a set of operations between PSVector and local double array.
 */
private[spark] class RemotePSVector (override val proxy: PSModelProxy) extends PSVector {
  import MergeType._
  import RemotePSVector._

  def toBreeze: BreezePSVector = new BreezePSVector(proxy)

  /**
   * Pull PSVector from PS nodes to local.
   * @param fromCache if false, it will pull the value from PS,
   *                  otherwise, if true, it will get the value from `localArrayCache` firstly.
   * @return
   */
  def pull(fromCache: Boolean = false): Array[Double] = {
    if (fromCache) {
      if (!localArrayCache.contains(proxy)) {
        localArrayCache.synchronized {
          if (!localArrayCache.contains(proxy)) {
            val localArray = PSClient().pull(proxy)
            localArrayCache.put(proxy, localArray)
          }
        }
      }
    } else {
      val localArray = PSClient().pull(proxy)
      if (localArrayCache.contains(proxy)) {
        localArrayCache(proxy) = localArray
      } else {
        localArrayCache.put(proxy, localArray)
      }
    }
    localArrayCache(proxy)
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
    require(delta.length == proxy.numDimensions)

    mergeCache.synchronized {
      init(INCREMENT, proxy)
      val mergedArray = mergeCache.get(proxy)._2
      PSClient().BLAS.daxpy(proxy.numDimensions, 1.0, delta, 1, mergedArray, 1)
    }
  }

  /**
   * Increment a local sparse Double array to PSVector
   * Notice: You should call `flush` to flush `mergedArray` result to PS nodes.
   */
  def increment(indices: Array[Int], values: Array[Double]): Unit = {
    require(indices.length == values.length && indices.length <= proxy.numDimensions)

    mergeCache.synchronized {
      init(INCREMENT, proxy)
      val mergedArray = mergeCache.get(proxy)._2
      var i = 0
      while (i < indices.length) {
        mergedArray(indices(i)) += values(i)
        i += 1
      }
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
    val dim = proxy.numDimensions
    require(other.length == dim)
    mergeCache.synchronized {
      init(MAX, proxy)
      val mergedArray = mergeCache.get(proxy)._2
      var i = 0
      while (i < dim) {
        mergedArray(i) = math.max(mergedArray(i), other(i))
        i += 1
      }
    }
  }

  /**
   * Find the maximum number of each dimension for PSVector and local sparse array.
   * Notice: You should call `flush` to flush `mergedArray` result to PS nodes.
   */
  def mergeMax(indices: Array[Int], values: Array[Double]): Unit = {
    val dim = proxy.numDimensions
    require(indices.length == values.length && indices.length <= dim)

    mergeCache.synchronized {
      init(MAX, proxy)

      val mergedArray = mergeCache.get(proxy)._2
      var i = 0
      while (i < indices.length) {
        val index = indices(i)
        mergedArray(index) = math.max(mergedArray(index), values(i))
        i += 1
      }
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
    require(other.length == proxy.numDimensions)
    mergeCache.synchronized {
      init(MIN, proxy)
      val mergedArray = mergeCache.get(proxy)._2
      var i = 0
      while (i < mergedArray.length) {
        mergedArray(i) = math.min(mergedArray(i), other(i))
        i += 1
      }
    }
  }

  /**
   * Find the minimum number of each dimension for PSVector and local sparse array.
   * Notice: You should call `flush` to flush `mergedArray` result to PS nodes.
   */
  def mergeMin(indices: Array[Int], values: Array[Double]): Unit = {
    val dim = proxy.numDimensions
    require(indices.length == values.length && indices.length <= dim)
    mergeCache.synchronized {
      init(MIN, proxy)
      val mergedArray = mergeCache.get(proxy)._2
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
   * Notice: You should call `flush` to flush `mergedArray` result to PS nodes.
   */
  def mergeMinAndFlush(other: Array[Double]): Unit = {
    PSClient().mergeMin(proxy, other)
  }

  def flush(): Unit = {
    if (TaskContext.get() == null) { // run flush on driver
      val sparkConf = SparkEnv.get.conf
      val executorNum = sparkConf.getInt("spark.executor.instances", 1)
      val core = sparkConf.getInt("spark.executor.cores", 1)
      val totalTask = core * executorNum
      val spark = SparkSession.builder().getOrCreate()
      spark.sparkContext.range(0, totalTask, 1, totalTask)
        .foreach { taskId =>
          RemotePSVector.flush(proxy)
        }
    } else { // run flush on executor
      RemotePSVector.flush(proxy)
    }
  }
}

object RemotePSVector {
  val localArrayCache = new scala.collection.mutable.WeakHashMap[PSModelProxy, Array[Double]]
  def keyString = {
    localArrayCache.keys
      .map(key => key.poolId + "_" + key.id)
      .mkString(",")
  }

  import MergeType._

  val mergeCache = new ConcurrentHashMap[PSModelProxy, (MergeType, Array[Double])]()

  private def init(mergeType: MergeType, proxy: PSModelProxy): Unit = {
    if (mergeCache.containsKey(proxy)) {
      require(mergeCache.get(proxy)._1 == mergeType,
        "Do not use different merge methods on the same RemotePSVector!")
    }

    mergeCache.putIfAbsent(proxy, {
      val initArray = mergeType match {
        case INCREMENT => Array.fill(proxy.numDimensions)(0.0)
        case MAX => Array.fill(proxy.numDimensions)(Double.MinValue)
        case MIN => Array.fill(proxy.numDimensions)(Double.MaxValue)
        case UNDEFINED => throw new Exception("undefined merge type")
      }
      (mergeType, initArray)
    })
  }

  private def flush(proxy: PSModelProxy) : Unit = {
    mergeCache.synchronized {
      if (mergeCache.containsKey(proxy)) {
        val (mergeType, mergedArray) = mergeCache.get(proxy)
        mergeType match {
          case INCREMENT => PSClient().increment(proxy, mergedArray)
          case MAX => PSClient().mergeMax(proxy, mergedArray)
          case MIN => PSClient().mergeMin(proxy, mergedArray)
        }
        mergeCache.remove(proxy)
      }
    }
  }

  private[spark] def flush(): Unit = {
    if (TaskContext.get() == null) { // run flush on driver
      val sparkConf = SparkEnv.get.conf
      val executorNum = sparkConf.getInt("spark.executor.instances", 1)
      val core = sparkConf.getInt("spark.executor.cores", 1)
      val totalTask = core * executorNum
      val spark = SparkSession.builder().getOrCreate()
      spark.sparkContext.range(0, totalTask, 1, totalTask)
        .foreach { taskId =>
          for (entry <- mergeCache.entrySet().asScala) {
            RemotePSVector.flush(entry.getKey)
          }
        }
    } else { // run flush on executor
      for (entry <- mergeCache.entrySet().asScala) {
        RemotePSVector.flush(entry.getKey)
      }
    }
  }
}

object MergeType extends Enumeration {
  type MergeType = Value
  val UNDEFINED, INCREMENT, MAX, MIN = Value
}
