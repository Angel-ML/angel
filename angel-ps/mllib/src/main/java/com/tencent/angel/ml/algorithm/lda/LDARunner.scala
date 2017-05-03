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

package com.tencent.angel.ml.algorithm.lda

import com.tencent.angel.client.AngelClientFactory
import com.tencent.angel.ml.algorithm.MLRunner
import org.apache.hadoop.conf.Configuration

class LDARunner extends MLRunner {

  /**
    * Training job to obtain a model
    */
  override
  def train(conf: Configuration): Unit = {

    conf.set("angel.task.user.task.class", classOf[LDATrainTask].getName)

    // Create an angel job client
    val client = AngelClientFactory.get(conf)

    // Submit this application
    client.submit()

    // Create a LDA model
    val lDAModel = new LDAModel(conf)

    // Model will be saved to HDFS, now set the path
    lDAModel.setSavePath(conf)

    // Load model meta to client
    client.loadModel(lDAModel)

    // Start
    client.start()

    // Run user task and wait for completion, user task is set in "angel.task.user.task.class"
    client.waitForCompletion()

    // Save the trained model to HDFS
    client.saveModel(lDAModel)

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
