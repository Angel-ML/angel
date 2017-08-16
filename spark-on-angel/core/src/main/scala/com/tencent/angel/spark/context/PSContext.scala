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

package com.tencent.angel.spark.context

import java.util.concurrent.ConcurrentHashMap

import com.tencent.angel.AngelDeployMode
import com.tencent.angel.ml.matrix.MatrixMeta
import com.tencent.angel.spark.model.PSModelPool
import com.tencent.angel.spark.model.matrix.MatrixType.MatrixType
import org.apache.spark._

import scala.collection.JavaConverters._
import scala.collection.Map

abstract class PSContext {

  private[spark] def conf: Map[String, String]

  /**
   * PSContext can create more than one PSVectorPool.
   */
  private val psModelPools = new ConcurrentHashMap[Int, PSModelPool]()

  /**
   * Create a vector pool in PS nodes.
   * Notice: it can only be called on driver.
   *
   * @param numDimensions dimension of vectors
   * @param capacity capacity of pool
   */
  def createModelPool(numDimensions: Int, capacity: Int): PSModelPool = {
    val pool = doCreateModelPool(numDimensions, capacity)
    psModelPools.put(pool.id, pool)
    pool
  }

  def createMatrix(rows: Int, cols: Int, t: MatrixType): MatrixMeta

  def destroyMatrix(meta: MatrixMeta): Unit

  /**
   * Destroy a vector pool in PS nodes.
   * Notice: it can only be called in th driver.
   *
   * @param pool the pool to destroy
   */
  def destroyModelPool(pool: PSModelPool): Unit = {
    doDestroyModelPool(pool)
    psModelPools.remove(pool.id)
  }

  def stop(): Unit = {
  }

  protected def doCreateModelPool(numDimensions: Int, capacity: Int): PSModelPool
  protected def doDestroyModelPool(pool: PSModelPool): Unit

  private[spark] def getPool(id: Int): PSModelPool = {
    psModelPools.get(id)
  }
}

object PSContext {
  private var context: PSContext = _
  private var failCause: Exception = _

  def getOrCreate(sc: SparkContext): PSContext = {
    val context = getOrCreate()
    context.conf.foreach {
      case (key, value) => sc.setLocalProperty(key, value)
    }
    context
  }

  /**
   * Get the PSContext instance
   * new `context` instance if it not exists
   */
  def getOrCreate(): PSContext = {
    if (instance == null) {
      throw new SparkException("PSContext init failed!", failCause)
    }
    instance
  }

  /**
   * Clean up PSContext.
   */
  def stop(): Unit = {
    for (entry <- context.psModelPools.entrySet().asScala) {
      context.destroyModelPool(entry.getValue)
    }

    val env = SparkEnv.get
    if (PSContext.isAngelMode(env.conf)) {
      AngelPSContext.stop()
    }
    PSContext.context.stop()
    PSContext.context = null
  }

  /**
   * new `context` singleton instance if it not exists
   *
   * @return
   */
  private def instance: PSContext = {
    if (context == null) {
      classOf[PSContext].synchronized {
        if (context == null) {
          try {
            val env = SparkEnv.get
            context = AngelPSContext(env.executorId, env.conf)
          } catch {
            case e: Exception =>
              context = null
              failCause = e
          }
        }
      }
    }
    context
  }

  private def isLocalMaster(conf: SparkConf): Boolean = {
    val master = conf.get("spark.master", "")
    master.toLowerCase.startsWith("local")
  }

  /**
   * For AngelPSContext, figure out PS Mode is LOCAL or YARN
   */
  private def isAngelMode(conf: SparkConf): Boolean = {
    val psMode = conf.getOption("spark.ps.mode")
    var isAngelContext = false
    if (!isLocalMaster(conf)) {
      isAngelContext = true
    } else {
      if (psMode.isDefined && psMode.get == AngelDeployMode.LOCAL.toString) {
        isAngelContext = true
      }
    }
    isAngelContext
  }


  private[spark] def getTaskId(): Int = {
    val tc = TaskContext.get()
    if (tc == null) -1 else tc.partitionId()
  }
}

