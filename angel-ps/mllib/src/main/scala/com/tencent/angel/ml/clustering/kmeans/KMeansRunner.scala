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


package com.tencent.angel.ml.clustering.kmeans

import com.tencent.angel.client.AngelClientFactory
import com.tencent.angel.ml.core.MLRunner
import com.tencent.angel.ml.core.conf.SharedConf
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration


object KMeansRunner {
  val KMEANS_CENTER_MAT = "centers"
  val KMEANS_OBJ_MAT = "obj"
}

class KMeansRunner extends MLRunner {

  private val LOG = LogFactory.getLog(classOf[KMeansRunner])

  /**
    * Training job to obtain a model
    */
  override def train(conf: Configuration): Unit = {
    conf.setInt("angel.worker.matrix.transfer.request.timeout.ms", 60000)
    val client = AngelClientFactory.get(conf)
    SharedConf.get(conf)
    val model = new KMeansModel(conf)

    try {
      client.startPSServer()
      client.loadModel(model)
      client.runTask(classOf[KMeansTrainTask])
      client.waitForCompletion()
      client.saveModel(model)
    } finally {
      client.stop()
    }
  }

  /**
    * Using a model to predict with unobserved samples
    */
  override def predict(conf: Configuration): Unit = {
    conf.setInt("angel.worker.matrix.transfer.request.timeout.ms", 60000)
    val client = AngelClientFactory.get(conf)
    SharedConf.get(conf)
    val model = new KMeansModel(conf)

    try {
      client.startPSServer()
      client.loadModel(model)
      client.runTask(classOf[KMeansPredictTask])
      client.waitForCompletion()
    } finally {
      client.stop()
    }
  }
}
