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

package com.tencent.angel.ml.matrixfactorization

import com.tencent.angel.client.AngelClientFactory
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.MLRunner
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

class MatrixFactorizationRunner extends MLRunner {
  var LOG = LogFactory.getLog(classOf[MatrixFactorizationRunner])

  /**
    * Training job to obtain a model
    */
  override def train(conf: Configuration): Unit = {
    conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, classOf[MFTrainTask].getName)

    // Create an angel job client
    val client = AngelClientFactory.get(conf)

    // Submit this application
    client.startPSServer()

    // Create a MFModel
    val mfModel = new MFModel(conf)

    // Load model meta to client
    client.loadModel(mfModel)

    // Start
    client.runTask(classOf[MFTrainTask])

    // Run user task and wait for completion
    // User task is set in AngelConf.ANGEL_TASK_USER_TASKCLASS
    client.waitForCompletion()

    // Save the trained model to HDFS
    client.saveModel(mfModel)

    // Stop
    client.stop()
  }

  /**
    * Using a model to predict with unobserved samples
    */
  override def predict(conf: Configuration): Unit = ???

  /**
    * Incremental training job to obtain a model based on a trained model
    */
  override def incTrain(conf: Configuration): Unit = ???
}
