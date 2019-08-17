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


package com.tencent.angel.ml.core.graphsubmit

import com.tencent.angel.client.AngelClientFactory
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.mlcore.conf.SharedConf
import com.tencent.angel.ml.core.utils.SConfHelper
import com.tencent.angel.mlcore.variable.VarState
import com.tencent.angel.ml.core.{AngelMasterContext, MLRunner}
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

class GraphRunner extends MLRunner with SConfHelper {

  private val LOG = LogFactory.getLog(classOf[GraphRunner])

  /**
    * Run model train task
    *
    * @param conf : configuration for resource
    */
  override def train(conf: Configuration): Unit = {
    val client = AngelClientFactory.get(conf)
    val envCtx = AngelMasterContext(client)
    val sharedConf = initConf(conf)

    val saveModelPath = sharedConf.get(AngelConf.ANGEL_SAVE_MODEL_PATH, "")
    val loadModelPath = sharedConf.get(AngelConf.ANGEL_LOAD_MODEL_PATH, "")

    try {
      client.startPSServer()

      val modelClassName: String = sharedConf.modelClassName
      val model: AngelModel = AngelModel(modelClassName, sharedConf)
      model.buildNetwork()

      model.createMatrices(envCtx)
      if (!loadModelPath.isEmpty) {
        model.loadModel(envCtx, loadModelPath, conf)
      } else {
        model.setState(VarState.Initialized)
      }

      client.runTask(classOf[GraphTrainTask])
      client.waitForCompletion()

      if (!saveModelPath.isEmpty) {
        model.saveModel(envCtx, saveModelPath)
      }
    } finally {
      client.stop()
    }
  }

  /**
    * Run model predict task
    *
    * @param conf : configuration for resource
    */
  override def predict(conf: Configuration): Unit = {
    val client = AngelClientFactory.get(conf)
    val envCtx = AngelMasterContext(client)
    val sharedConf = initConf(conf)

    val loadModelPath = sharedConf.getString(AngelConf.ANGEL_LOAD_MODEL_PATH, "")
    assert(!loadModelPath.isEmpty)

    try {
      client.startPSServer()

      val modelClassName: String = sharedConf.modelClassName
      val model: AngelModel = AngelModel(modelClassName, sharedConf)
      model.buildNetwork()

      model.createMatrices(envCtx)
      model.loadModel(envCtx, loadModelPath, conf)
      client.runTask(classOf[GraphPredictTask])
      client.waitForCompletion()
    } catch {
      case x: Exception => LOG.error("predict failed ", x)
    } finally {
      client.stop(0)
    }
  }
}
