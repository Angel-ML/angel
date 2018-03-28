/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.ml.regression.linear

import com.tencent.angel.client.AngelClientFactory
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.MLRunner
import org.apache.hadoop.conf.Configuration

/**
  * Run linear regression task on angel
  */

class LinearRegRunner extends MLRunner {

  /**
    * Run linear regression train task
    *
    * @param conf : configuration of algorithm and resource
    */
  override
  def train(conf: Configuration): Unit = {
    conf.setInt("angel.worker.matrix.transfer.request.timeout.ms", 60000)
    conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, classOf[LinearRegTrainTask].getName)

    // Create an angel job client
    val client = AngelClientFactory.get(conf)

    // Submit this application
    client.startPSServer()

    // Create a linear reg model
    val model = new LinearRegModel(conf)

    // Load model meta to client
    client.loadModel(model)

    // Run user task
    client.runTask(classOf[LinearRegTrainTask])

    // Wait for completion,
    // User task is set in AngelConf.ANGEL_TASK_USER_TASKCLASS
    client.waitForCompletion()

    // Save the trained model to HDFS
    client.saveModel(model)

    // Stop
    client.stop()
  }

  /**
    * Run linear regression  predict task
    *
    * @param conf : configuration of algorithm and resource
    */
  override
  def predict(conf: Configuration): Unit = {
    conf.setInt("angel.worker.matrix.transfer.request.timeout.ms", 60000)
    conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, classOf[LinearRegPredictTask].getName)

    // Create an angel job client
    val client = AngelClientFactory.get(conf)

    // Submit this application
    client.startPSServer()

    // Create a model
    val model = new LinearRegModel(conf)

    // Add the model meta to client
    client.loadModel(model)

    // Run user task
    client.runTask(classOf[LinearRegPredictTask])

    // Wait for completion,
    // User task is set in AngelConf.ANGEL_TASK_USER_TASKCLASS
    client.waitForCompletion()

    // Stop
    client.stop()
  }

  /*
   * Run incremental train task
   * @param conf: configuration of algorithm and resource
   */
  def incTrain(conf: Configuration): Unit = {
    conf.setInt("angel.worker.matrix.transfer.request.timeout.ms", 60000)

    val path = conf.get(AngelConf.ANGEL_LOAD_MODEL_PATH)
    if (path == null) throw new AngelException("parameter '" + AngelConf.ANGEL_LOAD_MODEL_PATH + "' should be set to load model")
    conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, classOf[LinearRegTrainTask].getName)

    // Create an angel job client
    val client = AngelClientFactory.get(conf)

    // Submit this application
    client.startPSServer()

    // Create a model
    val model = new LinearRegModel(conf)

    // Load model meta to client
    client.loadModel(model)

    // Run user task
    client.runTask(classOf[LinearRegTrainTask])

    // Wait for completion,
    // User task is set in AngelConf.ANGEL_TASK_USER_TASKCLASS
    client.waitForCompletion()

    // Save the incremental trained model to HDFS
    client.saveModel(model)

    // Stop
    client.stop()
  }
}

