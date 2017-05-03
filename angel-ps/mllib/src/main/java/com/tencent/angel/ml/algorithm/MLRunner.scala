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

package com.tencent.angel.ml.algorithm

import com.tencent.angel.AppSubmitter
import com.tencent.angel.ml.algorithm.conf.MLConf
import org.apache.hadoop.conf.Configuration


trait MLRunner extends AppSubmitter{

  /**
    * Training job to obtain a model
    */
  def train(conf: Configuration)

  /**
    * Incremental training job to obtain a model based on a trained model
    */
  def incTrain(conf: Configuration)

  /**
    * Using a model to predict with unobserved samples
    */
  def predict(conf: Configuration)




  @throws[Exception]
  override
  def submit(conf: Configuration): Unit = {
    val actType = conf.get(MLConf.ANGEL_ACTION_TYPE);
    actType match {
      case MLConf.ANGEL_ML_TRAIN =>
        train(conf)
      case MLConf.ANGEL_ML_PREDICT =>
        predict(conf)
      case MLConf.ANGEL_ML_INC_TRAIN =>
        incTrain(conf)
      case _ =>
        println("Error action type, should be train or predict")
        System.exit(1)
    }
  }

}
