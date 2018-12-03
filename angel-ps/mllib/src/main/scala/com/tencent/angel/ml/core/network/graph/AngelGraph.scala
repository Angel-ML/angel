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


package com.tencent.angel.ml.core.network.graph

import java.util

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.layers.PlaceHolder
import com.tencent.angel.ml.core.network.variable._
import com.tencent.angel.ml.matrix.MatrixContext
import com.tencent.angel.ml.core.utils.{GraphInvalidate, PSMatrixUtils, VariableInvalidate}
import com.tencent.angel.model.{ModelLoadContext, ModelSaveContext}
import org.apache.commons.logging.{Log, LogFactory}


class AngelGraph(placeHolder: PlaceHolder, conf: SharedConf) extends Graph(placeHolder, conf) with Serializable {

  def this(placeHolder: PlaceHolder) = this(placeHolder, SharedConf.get())

  private val LOG: Log = LogFactory.getLog(classOf[AngelGraph])

  override def createMatrices(envCtx: EvnContext): Unit = {
    envCtx match {
      case env: AngelEvnContext =>
        val contexts = new util.ArrayList[MatrixContext](variables.size)
        variables.foreach{
          case variable: PSBlasMatVariable => contexts.add(variable.getMatrixCtx)
          case variable: PSEmbedVariable => contexts.add(variable.getMatrixCtx)
          case variable: PSMatVariable => contexts.add(variable.getMatrixCtx)
          case variable: PSVecVariable => contexts.add(variable.getMatrixCtx)
          case _ => throw VariableInvalidate("Variable Invalidate, Only PS Variables Are Allowed")
        }

        env.client.createMatrices(contexts)
      case _ => throw GraphInvalidate("Graph Invalidate, Use AngelGraph Instead!")
    }
  }

  override def createMatrices(): Unit = {
    val matrixCtxs = variables.collect {
      case variable: PSBlasMatVariable => variable.getMatrixCtx
      case variable: PSEmbedVariable => variable.getMatrixCtx
      case variable: PSMatVariable => variable.getMatrixCtx
      case variable: PSVecVariable => variable.getMatrixCtx
    }

    PSMatrixUtils.createPSMatrix(matrixCtxs)
  }

  override def loadModel(envCtx: EvnContext, path: String): Unit = {
    envCtx match {
      case env: AngelEvnContext =>
        val loadContext = new ModelLoadContext(path)
        trainableLayer.foreach { layer => layer.loadParams(loadContext) }
        env.client.load(loadContext)
      case _ => throw GraphInvalidate("Graph Invalidate, Use AngelGraph Instead!")
    }

  }

  override def saveModel(envCtx: EvnContext, path: String): Unit = {
    envCtx match {
      case env: AngelEvnContext =>
        val saveContext = new ModelSaveContext(path)
        trainableLayer.foreach { layer => layer.saveParams(saveContext) }
        env.client.save(saveContext)
      case _ => throw GraphInvalidate("Graph Invalidate, Use AngelGraph Instead!")
    }
  }
}
