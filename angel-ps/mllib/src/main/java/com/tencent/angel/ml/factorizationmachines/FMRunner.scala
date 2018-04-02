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

package com.tencent.angel.ml.factorizationmachines

import com.tencent.angel.ml.MLRunner
import org.apache.hadoop.conf.Configuration

/**
  * Run Factorization Machines train task on Angel
  */
class FMRunner extends MLRunner {
  /**
    * Training job to obtain a FM model
    */
  override
  def train(conf: Configuration): Unit = {
    train(conf, FMModel(conf), classOf[FMTrainTask])
  }

  /**
    * Incremental training job to obtain FM model based on a trained model
    */
  override def incTrain(conf: Configuration): Unit = ???

  /**
    * Using a FM model to predict with unobserved samples
    */
  override def predict(conf: Configuration): Unit = {
    super.predict(conf, FMModel(conf), classOf[FMPredictTask])
  }
}
