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

package com.tencent.angel.ml.clustering.kmeans


import com.tencent.angel.client.AngelClientFactory
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.MLRunner
import com.tencent.angel.ml.conf.MLConf
import org.apache.hadoop.conf.Configuration

object KMeansRunner {
  val KMEANS_CENTER_MAT = "centers"
  val KMEANS_OBJ_MAT = "obj"
}

class KMeansRunner extends MLRunner {
  /**
    * Training job to obtain a model
    */
  override
  def train(conf: Configuration): Unit = {
    conf.setBoolean(MLConf.ML_DATA_IS_NEGY, false)
    conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, classOf[KMeansTrainTask].getName)

    // Create an angel job client
    val client = AngelClientFactory.get(conf)

    // Submit this application
    client.startPSServer()

    // Create a KMeans model
    val kmeansModel = new KMeansModel(conf)

    // Load model meta to client
    client.loadModel(kmeansModel)

    // Start
    client.runTask(classOf[KMeansTrainTask])

    // Run user task and wait for completion,
    // User task is set in AngelConf.ANGEL_TASK_USER_TASKCLASS
    client.waitForCompletion()

    // Save the trained model to HDFS
    client.saveModel(kmeansModel)

    // Stop
    client.stop()
  }

  /**
    * Using a model to predict with unobserved samples
    */
  override
  def predict(conf: Configuration): Unit = {
    conf.setBoolean(MLConf.ML_DATA_IS_NEGY, false)
    conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, classOf[KMeansPredictTask].getName)

    // Create an angel job client
    val client = AngelClientFactory.get(conf)

    // Submit this application
    client.startPSServer()

    // Create KMeans model
    val model = new KMeansModel(conf)

    // Add the model meta to client
    client.loadModel(model)

    // Start
    client.runTask(classOf[KMeansPredictTask])

    // Run user task and wait for completion,
    // User task is set in AngelConf.ANGEL_TASK_USER_TASKCLASS
    client.waitForCompletion()

    // Stop
    client.stop()
  }

  /**
    * Incremental training job to obtain a model based on a trained model
    */
  override def incTrain(conf: Configuration): Unit = ???
}
