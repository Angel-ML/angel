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
package com.tencent.angel.ml.core

import com.tencent.angel.client.AngelClient
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.mlcore.conf.SharedConf
import com.tencent.angel.mlcore.network.EnvContext
import com.tencent.angel.mlcore.variable.{VarState, VariableManager}
import com.tencent.angel.ml.core.variable.PSVariable
import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.model.{MatrixSaveContext, ModelSaveContext}
import com.tencent.angel.psagent.PSAgent
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConversions._

class PSVariableManager(isSparseFormat: Boolean, conf: SharedConf)
  extends VariableManager(isSparseFormat, conf) {

  override def createALL[T](envCtx: EnvContext[T]): Unit = {
    envCtx match {
      case AngelMasterContext(client: AngelClient) if client != null =>
        getALLVariables.foreach {
          case variable: PSVariable => client.addMatrix(variable.getMatrixCtx)
          case _ =>
        }
        client.createMatrices()
        getALLVariables.foreach { variable => variable.setState(VarState.Created) }
      case _ =>
        getALLVariables.foreach { variable => variable.create(envCtx) }
    }
  }

  override def loadALL[T](envCtx: EnvContext[T], path: String, conf: Configuration): Unit = {
    envCtx match {
      case AngelMasterContext(client: AngelClient) if client != null =>
        client.load()
        getALLVariables.foreach { variable => variable.setState(VarState.Initialized) }
      case _ =>
        getALLVariables.foreach { variable => variable.load(envCtx, path, null) }
    }
  }

  override def pullALL(epoch: Int, indices: Vector = null): Unit = {
    // val isSparseFormat = graph.dataFormat == "libsvm" || graph.dataFormat == "dummy"

    variables.values().foreach {
      case variable if isSparseFormat && variable.allowPullWithIndex =>
        variable.pull(epoch, indices)
      case variable => variable.pull(epoch)
    }
  }

  override def pull(name: String, epoch: Int = 0, indices: Vector = null): Unit = {
    // val isSparseFormat = graph.dataFormat == "libsvm" || graph.dataFormat == "dummy"

    val variable = getVariable(name)
    if (variable != null) {
      variable match {
        case v if isSparseFormat && v.allowPullWithIndex =>
          v.pull(epoch, indices)
        case v => v.pull(epoch)
      }
    }

  }

  override def saveALL[T](envCtx: EnvContext[T], path: String): Unit = {
    envCtx match {
      case AngelMasterContext(client: AngelClient) if client != null =>
        val saveContext = new ModelSaveContext
        getALLVariables.foreach { variable =>
          assert(variable.getState == VarState.Initialized || variable.getState == VarState.Ready)
          saveContext.addMatrix(new MatrixSaveContext(variable.name, variable.formatClassName))
        }
        saveContext.setSavePath(conf.get(AngelConf.ANGEL_SAVE_MODEL_PATH, ""))
        val deleteExistsFile = conf.getBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST,
          AngelConf.DEFAULT_ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST)
        client.save(saveContext, deleteExistsFile)
      case _ =>
        getALLVariables.foreach { variable => variable.save(envCtx, path) }
    }
  }

  override def releaseALL[T](envCtx: EnvContext[T]): Unit = {
    envCtx match {
      case ctx @ AngelWorkerContext(client: PSAgent) if client != null =>
        getALLVariables.foreach {
          case variable: PSVariable => variable.release(ctx)
          case _ =>
        }

        variables.clear()
        slots.clear()
      case _ => throw new Exception("envCtx error!")
    }
  }
}
