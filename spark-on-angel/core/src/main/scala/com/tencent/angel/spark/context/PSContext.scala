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
import com.tencent.angel.ml.matrix.{MatrixMeta, RowType}
import org.apache.spark._
import scala.collection.Map

import com.tencent.angel.exception.AngelException
import com.tencent.angel.spark.models.PSVector


abstract class PSContext {
  private[spark] def conf: Map[String, String]

  protected def stop()

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
}

object PSContext {
  private var _instance: PSContext = _
  private var failCause: Exception = _

  def getOrCreate(sc: SparkContext): PSContext = {
    _instance = instance()

    if (_instance == null) {
      throw new Exception(s"init PSContext failed, $failCause")
    }

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
              e.printStackTrace()
              throw new AngelException("init AngelPSContext fail, please check logs of master of angel")
          }
        }
      }
    }
    _instance
  }

  /**
    * Clean up PSContext.
    */
  def stop(): Unit = {
    PSContext._instance.stop()
    PSContext._instance = null

    AngelPSContext.stopAngel()
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
