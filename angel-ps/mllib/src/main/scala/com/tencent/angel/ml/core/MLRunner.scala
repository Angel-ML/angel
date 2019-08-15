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


package com.tencent.angel.ml.core

import com.tencent.angel.AppSubmitter
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.mlcore.conf.MLCoreConf
import org.apache.hadoop.conf.Configuration


trait MLRunner extends AppSubmitter {

  /**
    * Training job to obtain a model
    */
  def train(conf: Configuration)

  /**
    * Using a model to predict with unobserved samples
    */
  def predict(conf: Configuration)

  @throws[Exception]
  override
  def submit(conf: Configuration): Unit = {
    val actType = conf.get(AngelConf.ANGEL_ACTION_TYPE)
    actType match {
      case MLCoreConf.ANGEL_ML_TRAIN | MLCoreConf.ANGEL_ML_INC_TRAIN  =>
        train(conf)
      case MLCoreConf.ANGEL_ML_PREDICT =>
        predict(conf)
      case _ =>
        println("Error action type, should be train or predict or inctrain")
        System.exit(1)
    }
  }

}
