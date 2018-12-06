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


package com.tencent.angel.ml.core.local

import com.tencent.angel.ml.core.local.variables.{LocalBlasMatVariable, LocalEmbedVariable, LocalMatVariable, LocalVecVariable}
import com.tencent.angel.ml.core.network.{EvnContext, Graph}
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.utils.{GraphInvalidate, VariableInvalidate}
import org.apache.commons.logging.{Log, LogFactory}


case class LocalEvnContext() extends EvnContext


class LocalGraph(placeHolder: PlaceHolder) extends Graph(placeHolder,
  "com.tencent.angel.ml.core.network.variable.VariableProvider") with Serializable {

  val LOG: Log = LogFactory.getLog(classOf[LocalGraph])

  override def createMatrices(envCtx: EvnContext): Unit = createMatrices()

  override def createMatrices(): Unit = {
    variables.foreach {
      case variable: LocalBlasMatVariable => variable.create()
      case variable: LocalEmbedVariable => variable.create()
      case variable: LocalMatVariable => variable.create()
      case variable: LocalVecVariable => variable.create()
      case _ => throw throw VariableInvalidate("Variable Invalidate, Only Local Variables Are Allowed")
    }
  }

  override def loadModel(envCtx: EvnContext, path: String): Unit = {
    envCtx match {
      case _: LocalEvnContext =>
        val loadContext = new ModelLoadContext(path)
        trainableLayer.foreach { layer => layer.loadParams(loadContext) }
      case _ => throw GraphInvalidate("Graph Invalidate, Use AngelGraph Instead!")
    }
  }

  override def saveModel(envCtx: EvnContext, path: String): Unit = ???
}
