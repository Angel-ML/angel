
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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.algorithm.GBDT

import com.tencent.angel.client.AngelClientFactory
import com.tencent.angel.conf.AngelConfiguration
import com.tencent.angel.ml.algorithm.MLRunner
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

class GBDTRunner extends MLRunner {

  var LOG = LogFactory.getLog(classOf[GBDTRunner])

  var featureNum: Int = 0
  var featureNonzero: Int = 0
  var maxTreeNum: Int = 0
  var maxTreeDepth: Int = 0
  var splitNum: Int = 0
  var featureSampleRatio: Float = 0.0f

  override def train(conf: Configuration): Unit = {

    conf.set(AngelConfiguration.ANGEL_TASK_USER_TASKCLASS,
      classOf[GBDTTrainTask].getName)

    // Create an angel job client
    val client = AngelClientFactory.get(conf)

    // Submit this application
    client.submit()

    // Create a GBDT model
    val model = new GBDTModel(conf)

    // Model will be saved to HDFS, set the path
    model.setSavePath(conf)

    // Load model meta to client
    client.loadModel(model)

    // Start
    client.start()

    // Run user task and wait for completion,
    // User task is set in AngelConfiguration.ANGEL_TASK_USER_TASKCLASS
    client.waitForCompletion()

    // Save the trained model to HDFS
    client.saveModel(model)

    // Stop
    client.stop()
  }

  override def predict(conf: Configuration) {
    conf.set(AngelConfiguration.ANGEL_TASK_USER_TASKCLASS,
      classOf[GBDTPredictTask].getName)

    // Create an angel job client
    val client = AngelClientFactory.get(conf)

    // Submit this application
    client.submit()

    // Create GBDT model
    val model = new GBDTModel(conf)

    // A trained model will be loaded from HDFS to PS, set the HDFS path
    model.setLoadPath(conf)

    // Add the model meta to client
    client.loadModel(model)

    // Start
    client.start()

    // Run user task and wait for completion,
    // User task is set in AngelConfiguration.ANGEL_TASK_USER_TASKCLASS
    client.waitForCompletion()

    // Stop
    client.stop()
  }

  /**
    * Incremental training job to obtain a model based on a trained model
    */
  override def incTrain(conf: Configuration): Unit = {
    train(conf)
  }
}