
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

package com.tencent.angel.ml.algorithm.classification.svm

import com.tencent.angel.client.AngelClientFactory
import com.tencent.angel.conf.AngelConfiguration
import com.tencent.angel.ml.algorithm.MLRunner
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

class SVMRunner extends MLRunner {

  var LOG = LogFactory.getLog(classOf[SVMRunner])

  /**
    * Run SVM train task
    * @param conf: configuration of algorithm and resource
    */
  override def train(conf: Configuration): Unit = {
    conf.setInt("angel.worker.matrixtransfer.request.timeout.ms", 60000)
    conf.set(AngelConfiguration.ANGEL_TASK_USER_TASKCLASS, classOf[SVMTrainTask].getName)

    // Create an angel job client
    val client = AngelClientFactory.get(conf)

    // Submit this application
    client.submit()

    // Create a LR model
    val svmModel = new SVMModel(conf)

    // Model will be saved to HDFS, set the path
    svmModel.setSavePath(conf)

    // Load model meta to client
    client.loadModel(svmModel)

    // Start
    client.start()

    // Run user task and wait for completion,
    // User task is set in AngelConfiguration.ANGEL_TASK_USER_TASKCLASS
    client.waitForCompletion()

    // Save the trained model to HDFS
    client.saveModel(svmModel)

    // Stop
    client.stop()

  }

  /**
    * Run SVM predict task
    * @param conf: configuration of algorithm and resource
    */
  override def predict(conf: Configuration): Unit = {
    conf.setInt("angel.worker.matrixtransfer.request.timeout.ms", 60000)
    conf.set(AngelConfiguration.ANGEL_TASK_USER_TASKCLASS, classOf[SVMPredictTask].getName)

    // Create an angel job client
    val client = AngelClientFactory.get(conf)

    // Submit this application
    client.submit()

    // Create LR model
    val svmModel = new SVMModel(conf)

    // A trained model will be loaded from HDFS to PS, set the HDFS path
    svmModel.setLoadPath(conf)

    // Add the model meta to client
    client.loadModel(svmModel)

    // Start
    client.start()

    // Run user task and wait for completion,
    // User task is set in AngelConfiguration.ANGEL_TASK_USER_TASKCLASS
    client.waitForCompletion()

    // Stop
    client.stop()
  }

  /*
   * Run LR incremental train task
   * @param conf: configuration of algorithm and resource
   */
  def incTrain(conf: Configuration): Unit = {
    conf.setInt("angel.worker.matrixtransfer.request.timeout.ms", 60000)
    conf.set(AngelConfiguration.ANGEL_TASK_USER_TASKCLASS, classOf[SVMTrainTask].getName)

    // Create an angel job client
    val client = AngelClientFactory.get(conf)

    // Submit this application
    client.submit()

    // Create a LR model
    val svmModel = new SVMModel(conf)

    // The trained model will be loaded from HDFS to PS, set the HDFS path
    svmModel.setLoadPath(conf)

    // The incremental learned model will be saved to HDFS, set the path
    svmModel.setSavePath(conf)

    // Load model meta to client
    client.loadModel(svmModel)

    // Start
    client.start()

    // Run user task and wait for completion,
    // User task is set in AngelConfiguration.ANGEL_TASK_USER_TASKCLASS
    client.waitForCompletion()

    // Save the incremental trained model to HDFS
    client.saveModel(svmModel)

    // Stop
    client.stop()
  }
}
