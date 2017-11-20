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

import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.models.vector.PSVector
import scala.collection.mutable

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkEnv, TaskContext}


/**
 * PushManager will cache the result of Increment/mergeMax/mergeMin in spark driver or executor
 * local static memory. And flush the result to PS after all Increment/mergeMax/mergeMin finished.
 */

object PushMan {

  import MergeType._
  private def vectorOps = PSClient.instance().vectorOps

  private val mergeCache = new mutable.HashMap[(Int, Int, MergeType), Array[Double]]()

  def cacheSize: Int = mergeCache.size

  private[spark] def getFromIncrementCache(vector: PSVector): Array[Double] = {
    getCachedArray(vector, INCREMENT)
  }

  private[spark] def getFromMaxCache(vector: PSVector): Array[Double] = {
    getCachedArray(vector, MAX)
  }

  private[spark] def getFromMinCache(vector: PSVector): Array[Double] = {
    getCachedArray(vector, MIN)
  }

  def flushAll() = {
    flush(mergeCache.keys.toArray)
  }

  private[spark] def flush(poolId: Int, id: Int, mergeType: MergeType): Unit = {
    flush(Array(Tuple3(poolId, id, mergeType)))
  }

  private def getCachedArray(vector: PSVector, mergeType: MergeType): Array[Double] = {
    mergeType match {
      case INCREMENT =>
        cacheGet(vector, mergeType, 0.0)
      case MAX =>
        cacheGet(vector, mergeType, Double.MinValue)
      case MIN =>
        cacheGet(vector, mergeType, Double.MaxValue)
    }
  }

  private def cacheGet(vector: PSVector, mergeType: MergeType, defaultValue: Double): Array[Double] = {
    if (!mergeCache.contains((vector.poolId, vector.id, mergeType))) {
      mergeCache.synchronized {
        if (!mergeCache.contains((vector.poolId, vector.id, mergeType))) {
          mergeCache.put((vector.poolId, vector.id, mergeType), Array.fill(vector.dimension.toInt)(defaultValue))
        }
      }
    }
    mergeCache((vector.poolId, vector.id, mergeType))
  }

  private def flush(keys: Array[(Int, Int, MergeType)]): Unit = {
    if (TaskContext.get() == null) { // run flush on driver
      val sparkConf = SparkEnv.get.conf
      val executorNum = sparkConf.getInt("spark.executor.instances", 1)
      val core = sparkConf.getInt("spark.executor.cores", 1)
      val totalTask = core * executorNum
      val spark = SparkSession.builder().getOrCreate()
      spark.sparkContext.range(0, totalTask, 1, totalTask)
        .foreach { taskId =>
          flushKeys(keys)
        }
    } else { // run flush on executor
      flushKeys(keys)
    }
  }

  private def flushKeys(keys: Array[(Int, Int, MergeType)]) = {
    mergeCache.synchronized {
      keys.foreach { case (poolId, id, mergeType) =>
        if (mergeCache.contains((poolId, id, mergeType))) {
          val mergeArray = mergeCache((poolId, id, mergeType))
          mergeType match {
            case INCREMENT => {
              println(s"PushMan Increment.")
              vectorOps.increment(poolId, id, mergeArray)
            }
            case MAX => vectorOps.mergeMax(poolId, id, mergeArray)
            case MIN => vectorOps.mergeMin(poolId, id, mergeArray)
          }
          mergeCache.remove((poolId, id, mergeType))
        }
      }
    }
  }
}

object MergeType extends Enumeration {
  type MergeType = Value
  val INCREMENT, MAX, MIN = Value
}
