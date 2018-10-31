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
import com.tencent.angel.ml.core.MLRunner
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.utils.paramsutils.JsonUtils
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

class GraphRunner extends MLRunner {

  val LOG = LogFactory.getLog(classOf[GraphRunner])

  /**
    * Run model train task
    *
    * @param conf : configuration for resource
    */
  override def train(conf: Configuration): Unit = {
    val client = AngelClientFactory.get(conf)

    if (conf.get(AngelConf.ANGEL_ML_CONF) != null) {
      SharedConf.get(conf)
      JsonUtils.init()
    } else
      SharedConf.get(conf)

    val modelClassName: String = SharedConf.modelClassName
    val model: GraphModel = GraphModel(modelClassName, conf)
    val saveModelPath = conf.get(AngelConf.ANGEL_SAVE_MODEL_PATH, "")
    val loadModelPath = conf.get(AngelConf.ANGEL_LOAD_MODEL_PATH, "")

    model.buildNetwork()

    try {
      client.startPSServer()
      model.createMatrices(client)

      if (!loadModelPath.isEmpty)
        model.loadModel(client, loadModelPath)

      client.runTask(classOf[GraphTrainTask])
      client.waitForCompletion()

      if (!saveModelPath.isEmpty)
        model.saveModel(client, saveModelPath)
    } finally {
      client.stop()
    }
  }

  /**
   * Run model predict task
   * @param conf: configuration for resource
   */
  override def predict(conf: Configuration): Unit = {
    val client = AngelClientFactory.get(conf)
    if (conf.get(AngelConf.ANGEL_ML_CONF) != null) {
      SharedConf.get(conf)
      JsonUtils.init()
    } else {
      SharedConf.get(conf)
    }

    val modelClassName: String = SharedConf.modelClassName
    val model: GraphModel = GraphModel(modelClassName, conf)
    model.buildNetwork()
    val loadModelPath = conf.get(AngelConf.ANGEL_LOAD_MODEL_PATH, "")

    try {
      client.startPSServer()
      model.createMatrices(client)
      if (!loadModelPath.isEmpty)
        model.loadModel(client, loadModelPath)
      client.runTask(classOf[GraphPredictTask])
      client.waitForCompletion()
    } catch {
      case x:Exception => LOG.error("predict failed ", x)
    } finally {
      client.stop(0)
    }
  }
}
