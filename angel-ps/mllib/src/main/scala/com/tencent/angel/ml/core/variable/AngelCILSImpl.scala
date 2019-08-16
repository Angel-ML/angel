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

import java.util

import com.tencent.angel.client.AngelClient
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.ml.core.{AngelMasterContext, AngelWorkerContext}
import com.tencent.angel.mlcore.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.mlcore.network.EnvContext
import com.tencent.angel.model._
import com.tencent.angel.psagent.PSAgent

class AngelCILSImpl(val conf: SharedConf) extends CILSImpl {

  def doCreate[T](mCtx: MatrixContext, envCtx: EnvContext[T]): Unit = {
    envCtx match {
      case AngelMasterContext(client: AngelClient) if client != null =>
        val mcList = new util.ArrayList[MatrixContext]()
        mcList.add(mCtx)
        client.createMatrices(mcList)
      case _ =>
    }
  }

  override def doInit[T](mCtx: MatrixContext, envCtx: EnvContext[T], taskFlag: Int): Unit = ???

  override def doLoad[T](mCtx: MatrixContext, envCtx: EnvContext[T], path: String): Unit = {
    envCtx match {
      case AngelMasterContext(client: AngelClient) if client != null =>
        val loadContext = new ModelLoadContext(path)
        loadContext.addMatrix(new MatrixLoadContext(mCtx.getName))
        client.load(loadContext)
      case _ =>
    }
  }

  override def doSave[T](mCtx: MatrixContext, indices: Array[Int],
                         envCtx: EnvContext[T], path: String): Unit = {
    envCtx match {
      case AngelMasterContext(client: AngelClient) if client != null =>
        val saveContext: ModelSaveContext = new ModelSaveContext(path)
        val msc: MatrixSaveContext = new MatrixSaveContext(mCtx.getName,
          mCtx.getAttributes.get(MLCoreConf.ML_MATRIX_OUTPUT_FORMAT))
        msc.addIndices(indices)
        saveContext.addMatrix(msc)

        if (PSVariable.isFirstSave.getAndSet(false)) {
          val deleteExistsFile = conf.getBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST,
            AngelConf.DEFAULT_ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST)
          client.save(saveContext, deleteExistsFile)
        } else {
          client.save(saveContext, false)
        }
      case _ =>
    }
  }

  def doRelease[T](mCtx: MatrixContext, envCtx: EnvContext[T]): Unit = {
    envCtx match {
      case AngelWorkerContext(client: PSAgent) if client != null =>
        client.releaseMatrix(mCtx.getName)
      case _ =>
    }
  }
}
