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


package com.tencent.angel.spark.context

import com.tencent.angel.AngelDeployMode
import com.tencent.angel.ml.matrix.{MatrixContext, MatrixMeta, RowType}
import org.apache.spark._

import scala.collection.Map
import com.tencent.angel.exception.AngelException
import com.tencent.angel.model.{ModelLoadContext, ModelSaveContext}
import com.tencent.angel.spark.models.PSVector
import org.apache.commons.logging.{Log, LogFactory}

import java.util.concurrent.atomic.AtomicBoolean


abstract class PSContext {
  private[spark] def conf: Map[String, String]

  protected def stop()

  def createMatrix(matrixContext : MatrixContext): MatrixMeta

  def createMatrix(rows: Int, cols: Long, validIndexNum: Long, rowInBlock: Int, colInBlock: Long,
                   rowType: RowType, additionalConfiguration:Map[String, String] = Map()): MatrixMeta

  def createDenseMatrix(rows: Int, cols: Long, rowInBlock: Int, colInBlock: Long,
                        rowType: RowType = RowType.T_DOUBLE_DENSE,
                        additionalConfiguration:Map[String, String] = Map()): MatrixMeta

  def createSparseMatrix(rows: Int, cols: Long, range: Long, rowInBlock: Int, colInBlock: Long,
                         rowType: RowType = RowType.T_DOUBLE_SPARSE,
                         additionalConfiguration:Map[String, String] = Map()): MatrixMeta

  def destroyMatrix(matrixId: Int)

  def createVector(dim: Long, t: RowType, poolCapacity: Int, range: Long,
                   additionalConfiguration:Map[String, String] = Map()): PSVector

  def duplicateVector(originVector: PSVector): PSVector

  def destroyVector(vector: PSVector)

  def destroyVectorPool(vector: PSVector): Unit

  def refreshMatrix(): Unit

  def getMatrixMeta(matrixId: Int): Option[MatrixMeta]

  def save(ctx: ModelSaveContext)

  def checkpoint(checkpointId: Int, ctx: ModelSaveContext)

  def load(ctx: ModelLoadContext)

  def recover(checkpointId: Int, ctx: ModelLoadContext)

  def waitPS()
}

object PSContext {
  val LOG: Log = LogFactory.getLog(classOf[PSContext])
  private var _instance: PSContext = _
  private var failCause: Exception = _
  var cost: Long = _
  val stopped: AtomicBoolean = new AtomicBoolean(false)

  def getOrCreate(sc: SparkContext): PSContext = {
    cost = System.currentTimeMillis()
    _instance = instance()
    cost = System.currentTimeMillis() - cost
    println(s"wait ps start cost: ${cost} ms")

    if (_instance == null) {
      throw new Exception(s"init PSContext failed, $failCause")
    }
    stopped.set(false)

    _instance.conf.foreach {
      case (key, value) => sc.setLocalProperty(key, value)
    }

    _instance
  }

  def instance(): PSContext = {
    if (_instance == null) {
      classOf[PSContext].synchronized {
        if (_instance == null) {
          try {
            val env = SparkEnv.get
            _instance = AngelPSContext(env.executorId, env.conf)
          } catch {
            case e: Exception =>
              _instance = null
              // Surface the real cause in BOTH the executor stderr (printStackTrace)
              // AND the AngelException message itself, so it shows up in the driver
              // log too. Otherwise the only thing the driver sees is the canned
              // "init AngelPSContext fail, please check logs of master of angel"
              // which forces a manual SSH into the worker pod to diagnose. Wrapping
              // the real exception as `cause` also preserves the full stack for any
              // log4j appender that knows how to render Throwable.getCause.
              LOG.error("init AngelPSContext failed", e)
              e.printStackTrace()
              val msg = s"init AngelPSContext fail (root cause: ${e.getClass.getSimpleName}: ${e.getMessage}). " +
                "See executor stderr / Angel master log for the full stack trace."
              throw new AngelException(msg, e)
          }
        }
      }
    }
    _instance
  }

  def waitPS(sc: SparkContext): Unit = {
    if (_instance == null) {
      throw new Exception(s"init PSContext failed, $failCause")
    } else {
      try {
        _instance.waitPS()
        _instance.conf.foreach {
          case (key, value) => sc.setLocalProperty(key, value)
        }
      } catch {
        case e: Exception =>
          _instance = null
          e.printStackTrace()
          throw new AngelException("init AngelPSContext fail, please check logs of master of angel")
      }
    }
  }

  /**
    * Clean up PSContext.
    */
  def stop(): Unit = {
    if (stopped.compareAndSet(false, true)) {
      LOG.info("Start to stop ps")
      try {
        if (PSContext._instance != null) {
          PSContext._instance.stop()
        }
      } finally {
        PSContext._instance = null
        AngelPSContext.stopAngel()
      }
    }
  }

  private def isLocalMode(conf: SparkConf): Boolean = {
    val master = conf.get("spark.master", "")
    master.toLowerCase.startsWith("local")
  }

  private def isAngelMode(conf: SparkConf): Boolean = {
    if (isLocalMode(conf))
      return false

    val psMode = conf.getOption("spark.ps.mode")
    if (psMode.isDefined && psMode.get == AngelDeployMode.YARN.toString) {
      true
    } else {
      false
    }
  }

  private[spark] def getTaskId: Int = {
    val tc = TaskContext.get()
    if (tc == null) -1 else tc.partitionId()
  }
}
