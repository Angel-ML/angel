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

package com.tencent.angel.spark.models.vector.cache

import scala.collection.mutable

import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkEnv, TaskContext}

import com.tencent.angel.spark.linalg.{DenseVector, SparseVector, Vector}
import com.tencent.angel.spark.models.vector.VectorType.VectorType
import com.tencent.angel.spark.models.vector.{DensePSVector, PSVector, SparsePSVector, VectorType}


/**
 * PushManager will cache the result of Increment/mergeMax/mergeMin in spark driver or executor
 * local static memory. And flush the result to PS after all Increment/mergeMax/mergeMin finished.
 */

object PushMan {
  import MergeType._

  case class CacheKey(poolId: Int, id: Int, mType: MergeType, vType: VectorType) {
    def this(vector: PSVector, mType: MergeType) {
      this(vector.poolId, vector.id, mType,
        if (vector.getComponent.isInstanceOf[DensePSVector]) VectorType.DENSE else VectorType.SPARSE
      )
    }
  }

  private val mergeCache = new mutable.HashMap[CacheKey, Vector]()

  def cacheSize: Int = mergeCache.size

  private[spark] def getFromIncrementCache(vector: PSVector): Vector = {
    getCachedArray(vector, INCREMENT)
  }

  private[spark] def getFromMaxCache(vector: PSVector): Vector = {
    getCachedArray(vector, MAX)
  }

  private[spark] def getFromMinCache(vector: PSVector): Vector = {
    getCachedArray(vector, MIN)
  }

  def flushAll() = {
    flush(mergeCache.keys.toArray)
  }

  private[spark] def flush(vector: PSVector, mergeType: MergeType): Unit = {
    flush(Array(new CacheKey(vector, mergeType)))
  }

  private def getCachedArray(vector: PSVector, mergeType: MergeType): Vector = {
    mergeType match {
      case INCREMENT =>
        cacheGet(vector, mergeType, 0.0)
      case MAX =>
        cacheGet(vector, mergeType, Double.MinValue)
      case MIN =>
        cacheGet(vector, mergeType, Double.MaxValue)
    }

  }

  private def cacheGet(vector: PSVector, mergeType: MergeType, default: Double): Vector = {
    def defaultVector(vector: PSVector, defaultValue: Double): Vector = {
      vector.getComponent match {
        case dv: DensePSVector =>
          new DenseVector(Array.fill(dv.dimension.toInt)(defaultValue))
        case sv: SparsePSVector =>
          val keyValues = new Long2DoubleOpenHashMap()
          keyValues.defaultReturnValue(defaultValue)
          new SparseVector(sv.dimension, keyValues)
      }
    }

    val cacheKey = new CacheKey(vector, mergeType)
    if (!mergeCache.contains(cacheKey)) {
      mergeCache.synchronized {
        if (!mergeCache.contains(cacheKey)) {
          mergeCache.put(cacheKey, defaultVector(vector, default))
        }
      }
    }
    mergeCache(cacheKey)
  }

  private def flush(keys: Array[CacheKey]): Unit = {
    if (TaskContext.get() == null) { // run flush on driver
      val sparkConf = SparkEnv.get.conf
      val executorNum = sparkConf.getInt("spark.executor.instances", 1)
      val core = sparkConf.getInt("spark.executor.cores", 1)
      val totalTask = core * executorNum
      val spark = SparkSession.builder().getOrCreate()
      val bcKeys = spark.sparkContext.broadcast(keys)
      spark.sparkContext.range(0, totalTask, 1, totalTask)
        .foreach { taskId =>
          flushKeys(bcKeys.value)
        }
      flushKeys(keys)
    } else { // run flush on executor
      flushKeys(keys)
    }
  }

  private def flushKeys(keys: Array[CacheKey]) = {
    mergeCache.synchronized {
      keys.foreach { cacheKey =>
        if (mergeCache.contains(cacheKey)) {
          val mergeArray = mergeCache(cacheKey)
          cacheKey.mType match {
            case INCREMENT =>
              Local2RemoteOps.increment(cacheKey.poolId, cacheKey.id, cacheKey.vType, mergeArray)
            case MAX =>
              Local2RemoteOps.mergeMax(cacheKey.poolId, cacheKey.id, cacheKey.vType, mergeArray)
            case MIN =>
              Local2RemoteOps.mergeMin(cacheKey.poolId, cacheKey.id, cacheKey.vType, mergeArray)
          }
          mergeCache.remove(cacheKey)
        }
      }
    }
  }
}

object MergeType extends Enumeration {
  type MergeType = Value
  val INCREMENT, MAX, MIN = Value
}
