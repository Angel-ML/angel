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
package com.tencent.angel.ml.core.variable


import java.util.concurrent.atomic.AtomicBoolean

import com.tencent.angel.client.AngelPSClient
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.ml.core.conf.AngelMLConf
import com.tencent.angel.mlcore.conf.SharedConf
import com.tencent.angel.mlcore.network.EnvContext
import com.tencent.angel.ml.core.utils.PSMatrixUtils
import com.tencent.angel.ml.math2.utils.RowType
import com.tencent.angel.mlcore.variable._
import com.tencent.angel.psagent.PSAgent
import org.apache.hadoop.conf.Configuration


abstract class PSVariable(name: String,
                          rowType: RowType,
                          updater: Updater,
                          formatClassName: String,
                          allowPullWithIndex: Boolean)
                         (implicit conf: SharedConf, variableManager: VariableManager, cilsImpl: CILSImpl)
  extends Variable(name, rowType, updater, formatClassName, allowPullWithIndex) {

  protected var matrixId: Int = -1
  protected var ctx: MatrixContext = _

  def getMatrixId: Int = matrixId

  val numFactors: Int

  def getMatrixCtx: MatrixContext

  protected def rowsSaved(withSlot: Boolean = false): Array[Int]

  protected def doCreate[T](envCtx: EnvContext[T]): Unit = {
    if (envCtx != null && envCtx.client != null) {
      envCtx.client match {
        case _: PSAgent =>
          if (matrixId == -1) {
            matrixId = PSMatrixUtils.getMatrixId(name)
          }

          if (ctx == null) {
            ctx = getMatrixCtx
          }
        case _ =>
          cilsImpl.doCreate(getMatrixCtx, envCtx)
      }
    } else {
      // create matrix in work, on worker
      // in fact, the matrix has created, just get the matrixId here
      if (matrixId == -1) {
        matrixId = PSMatrixUtils.getMatrixId(name)
      }

      if (ctx == null) {
        ctx = getMatrixCtx
      }
    }
  }

  override def init(taskFlag: Int, mean: Double, stddev: Double): Unit = {
    writeLock.lock()

    try {
      this.mean = mean
      this.stddev = stddev

      if (state == VarState.Created) {
        val loadModelPath = conf.get(AngelConf.ANGEL_LOAD_MODEL_PATH, "")
        if (taskFlag == 0 && rowType.isDense && loadModelPath.isEmpty) {
          if (matrixId == -1) {
            matrixId = PSMatrixUtils.getMatrixId(name)
          }

          doInit(taskFlag)
        }

        // trans stats
        transSate(VarState.Created, VarState.Initialized)
      }

      assert(state == VarState.Initialized)
    } finally {
      writeLock.unlock()
    }
  }

  // call only on client
  protected override def doLoad[T](envCtx: EnvContext[T], path: String, conf: Configuration): Unit = {
    if (envCtx != null && envCtx.client != null) {
      envCtx.client match {
        case _: PSAgent =>
          if (matrixId == -1) {
            matrixId = PSMatrixUtils.getMatrixId(name)
          }

          if (ctx == null) {
            ctx = getMatrixCtx
          }
        case _ =>
          cilsImpl.doLoad(getMatrixCtx, envCtx, path)
      }
    } else {
      // create matrix in work, on worker
      // in fact, the matrix has created, just get the matrixId here
      if (matrixId == -1) {
        matrixId = PSMatrixUtils.getMatrixId(name)
      }

      if (ctx == null) {
        ctx = getMatrixCtx
      }
    }
  }

  protected override def doSave[T](envCtx: EnvContext[T], path: String): Unit = {
    if (envCtx != null && envCtx.client != null) {
      envCtx.client match {
        case _: PSAgent =>
          if (matrixId == -1) {
            matrixId = PSMatrixUtils.getMatrixId(name)
          }

          if (ctx == null) {
            ctx = getMatrixCtx
          }
        case _ =>
          val withSlot: Boolean = conf.getBoolean(AngelMLConf.ML_VERABLE_SAVE_WITHSLOT,
            AngelMLConf.DEFAULT_ML_VERABLE_SAVE_WITHSLOT)
          val indices = rowsSaved(withSlot)
          cilsImpl.doSave(getMatrixCtx, indices, envCtx, path)
      }
    }
  }

  protected override def doRelease[T](envCtx: EnvContext[T]): Unit = {
    if (envCtx != null && envCtx.client != null) {
      envCtx.client match {
        case _: PSAgent =>
          if (matrixId == -1) {
            matrixId = PSMatrixUtils.getMatrixId(name)
          }

          if (ctx == null) {
            ctx = getMatrixCtx
          }
        case _ =>
          cilsImpl.doRelease(getMatrixCtx, envCtx)
      }
    }
  }
}

object PSVariable {
  var isFirstSave: AtomicBoolean = new AtomicBoolean(true)
}
