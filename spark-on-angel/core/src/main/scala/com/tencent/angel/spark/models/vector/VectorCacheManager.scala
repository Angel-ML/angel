/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.spark.models.vector

import scala.collection.mutable

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkEnv, TaskContext}

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.matrix.psf.update.{MaxA, MinA}
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc
import com.tencent.angel.psagent.matrix.{MatrixClient, MatrixClientFactory}
import com.tencent.angel.spark.context.PSContext

/**
  * Cache is a Cache which cache PSVector in local static memory.
  */

object VectorCacheManager {
  val pullCache = new scala.collection.mutable.HashMap[(Int, Int), Vector]
  val mergeCache = new mutable.HashMap[CacheKey, Vector]()
  val INCREMENT = 0
  val MAX = 1
  val MIN = 2

  def release(vector: PSVector): Unit = {
    if (SparkEnv.get.executorId == "driver") {
      // run flush on driver
      val totalTask = getNumParallel
      SparkSession.builder().getOrCreate().sparkContext.range(0, totalTask, 1, totalTask).foreach { _ =>
        deleteVector(vector.poolId, vector.id)
      }
      deleteVector(vector.poolId, vector.id)
    } else {
      // run flush on executor
      deleteVector(vector.poolId, vector.id)
    }
  }

  private def getNumParallel: Int = {
    val sparkConf = SparkEnv.get.conf
    val executorNum = sparkConf.getOption("spark.executor.instances")
      .map(_.toInt)
      .filter(_ > 0)
      .getOrElse(sparkConf.getInt("spark.dynamicAllocation.maxExecutors", 1))
    val core = sparkConf.getInt("spark.executor.cores", 1)
    executorNum * core
  }

  def flushAll(userDefinedUpdateOpMap: Option[Map[Int, Vector => UpdateFunc]] = None): Unit = {
    val executorId = SparkEnv.get.executorId
    if (executorId != "driver")
      throw new AngelException("flushAll can only called by driver, use flush instead to flush on worker")
    val spark = SparkSession.builder().getOrCreate()
    val parallel = getNumParallel
    spark.sparkContext.range(0, parallel, 1, parallel).foreach { _ =>
      flush(userDefinedUpdateOpMap)
    }
    flush(userDefinedUpdateOpMap)
  }

  def flush(userDefinedUpdateOpMap: Option[Map[Int, Vector => UpdateFunc]] = None): Unit = {
    mergeCache.keys.foreach { cacheKey =>
      flush(cacheKey, userDefinedUpdateOpMap.map(funcs => funcs(cacheKey.mergeFuncId)))
    }
  }

  private[vector] def flush(cacheKey: CacheKey,
                            userDefinedUpdateOp: Option[Vector => UpdateFunc]): Unit = {
    mergeCache.synchronized {
      if (mergeCache.contains(cacheKey)) {
        val cached = mergeCache(cacheKey)
        userDefinedUpdateOp.fold(cacheKey.mergeFuncId match {
          case INCREMENT => vectorPoolClient(cacheKey.poolId).increment(cached)
          case MAX => vectorPoolClient(cacheKey.poolId).update(new MaxA(cacheKey.cacheId, cacheKey.id, cached)).get
          case MIN => vectorPoolClient(cacheKey.poolId).update(new MinA(cacheKey.cacheId, cacheKey.id, cached)).get
        })(func => vectorPoolClient(cacheKey.poolId).update(func(cached)).get)
        mergeCache.remove(cacheKey)
      }
    }
  }

  private def vectorPoolClient(poolId: Int): MatrixClient = {
    PSContext.instance()
    MatrixClientFactory.get(poolId, PSContext.getTaskId())
  }

  def aggregateCache(vector: Vector,
                     cacheKey: CacheKey,
                     userDefinedMergeOp: Option[(Vector, Vector) => Vector] = None): Unit = {
    if (!mergeCache.contains(cacheKey))
      mergeCache.synchronized {
        if (!mergeCache.contains(cacheKey)) mergeCache.put(cacheKey, vector)
      }
    else {
      val cached = mergeCache(cacheKey)
      cached.synchronized {
        userDefinedMergeOp.fold(cacheKey.mergeFuncId match {
          case INCREMENT => Ufuncs.iaxpy(cached, vector, 1.0)
          case MAX => Ufuncs.imax(cached, vector)
          case MIN => Ufuncs.imin(cached, vector)
        })(_.apply(cached, vector))
      }
    }
  }

  /*
  auto delete pull cache
   */
  private[spark] def autoRelease(poolId: Int, id: Int): Unit = {
    if (TaskContext.get() == null) {
      // run flush on driver
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
    } else {
      // run flush on executor
      deleteVector(poolId, id)
    }
  }

  /**
    * delete pull cache
    */
  private def deleteVector(poolId: Int, id: Int): Unit = {
    if (pullCache.contains((poolId, id))) {
      pullCache.synchronized {
        if (pullCache.contains((poolId, id))) {
          pullCache.remove((poolId, id))
        }
      }
    }
  }

  /**
    *
    * @param cacheId     id to distinguish caches
    * @param poolId      id to distinguish vector pool
    * @param id          id to distinguish vector
    * @param mergeFuncId id to distinguish mergeOp, 0 for increment, 1 for max, 2 for min and others for user defined
    */
  case class CacheKey(cacheId: Int, poolId: Int, id: Int, mergeFuncId: Int) {
    def apply(vector: Vector, cacheId: Int = 0, mergeFuncId: Int): CacheKey = CacheKey(cacheId, poolId, id, mergeFuncId)
  }

}
