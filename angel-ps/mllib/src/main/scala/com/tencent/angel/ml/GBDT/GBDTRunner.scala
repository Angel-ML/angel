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


package com.tencent.angel.ml.GBDT

import com.tencent.angel.client.AngelClientFactory
import com.tencent.angel.ml.core.MLRunner
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

class GBDTRunner extends MLRunner {

  val LOG = LogFactory.getLog(classOf[GBDTRunner])

  var featureNum: Int = 0
  var featureNonzero: Int = 0
  var maxTreeNum: Int = 0
  var maxTreeDepth: Int = 0
  var splitNum: Int = 0
  var featureSampleRatio: Float = 0.0f

  override def train(conf: Configuration): Unit = {

    val client = AngelClientFactory.get(conf)
    val model = new GBDTModel(conf)

    try {
      client.startPSServer()
      client.loadModel(model)
      client.runTask(classOf[GBDTTrainTask])
      client.waitForCompletion()
      client.saveModel(model)
    } finally {
      client.stop()
    }


  }

  override def predict(conf: Configuration) {
    conf.setInt("angel.worker.matrix.transfer.request.timeout.ms", 60000)

    val client = AngelClientFactory.get(conf)
    val model = new GBDTModel(conf)

    try {
      client.startPSServer()
      client.loadModel(model)
      client.runTask(classOf[GBDTPredictTask])
      client.waitForCompletion()
    } finally {
      client.stop()
    }


  }
}