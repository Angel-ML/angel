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

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkEnv, TaskContext}

import com.tencent.angel.spark.linalg.Vector
import com.tencent.angel.spark.models.vector.PSVector

/**
 * PullManager is a Cache which cache PSVector in local static memory.
 */
// TODO: support SparseVector and matrix
object PullMan {
  private val pullCache = new scala.collection.mutable.HashMap[(Int, Int), Vector]

  def cacheSize: Int = pullCache.size

  def pullFromCache(vector: PSVector): Vector = {
    if (!pullCache.contains((vector.poolId, vector.id))) {
      pullCache.synchronized {
        if (!pullCache.contains((vector.poolId, vector.id))) {
          val local = Local2RemoteOps.pull(vector)
          pullCache.put((vector.poolId, vector.id), local)
        }
      }
    }
    pullCache((vector.poolId, vector.id))
  }

  private[spark] def autoRelease(poolId: Int, id: Int): Unit = {
    if (TaskContext.get() == null) { // run flush on driver
      // TODO: can not run spark task in CleanTask
      /*
      val sparkConf = SparkEnv.get.conf
      val executorNum = sparkConf.getInt("spark.executor.instances", 1)
      val core = sparkConf.getInt("spark.executor.cores", 1)
      val totalTask = core * executorNum
      val spark = SparkSession.builder().getOrCreate()
      spark.sparkContext.range(0, totalTask, 1, totalTask)
        .foreach { taskId =>

        }
       */
      deleteVector(poolId, id)
    } else { // run flush on executor
      deleteVector(poolId, id)
    }
  }

  def release(vector: PSVector): Unit = {
    if (TaskContext.get() == null) { // run flush on driver
      val sparkConf = SparkEnv.get.conf
      val executorNum = sparkConf.getInt("spark.executor.instances", 1)
      val core = sparkConf.getInt("spark.executor.cores", 1)
      val totalTask = core * executorNum
      val spark = SparkSession.builder().getOrCreate()
      spark.sparkContext.range(0, totalTask, 1, totalTask)
      .foreach { taskId =>
        deleteVector(vector.poolId, vector.id)
      }
      deleteVector(vector.poolId, vector.id)
    } else { // run flush on executor
      deleteVector(vector.poolId, vector.id)
    }
  }

  private def deleteVector(poolId: Int, id: Int): Unit = {
    if (pullCache.contains((poolId, id))) {
      pullCache.synchronized {
        if (pullCache.contains((poolId, id))) {
          pullCache.remove((poolId, id))
        }
      }
    }
  }
}