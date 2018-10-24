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
import com.tencent.angel.client.AngelClientFactory
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.core.conf.MLConf
import com.tencent.angel.ml.model.MLModel
import com.tencent.angel.worker.task.BaseTask
import org.apache.hadoop.conf.Configuration


trait MLRunner extends AppSubmitter {

  /**
    * Training job to obtain a model
    */
  def train(conf: Configuration)

  /**
    * Default train method with standard training process. Don't Override this method.
    *
    * @param conf
    * @param model
    * @param taskClass
    */
  final protected def train(conf: Configuration, model: MLModel, taskClass: Class[_ <: BaseTask[_, _, _]]): Unit = {
    val client = AngelClientFactory.get(conf)

    try {
      client.startPSServer()
      client.loadModel(model)
      client.runTask(taskClass)
      client.waitForCompletion()
      client.saveModel(model)
    } finally {
      client.stop()
    }
  }

  /**
    * Using a model to predict with unobserved samples
    */
  def predict(conf: Configuration)

  /**
    * Default predict method with standard predict process. Don't try to override this Method.
    *
    * @param conf
    * @param model
    * @param taskClass
    */
  final protected def predict(conf: Configuration, model: MLModel, taskClass: Class[_ <: BaseTask[_, _, _]]): Unit = {
    val client = AngelClientFactory.get(conf)
    try {
      client.startPSServer()
      client.loadModel(model)
      client.runTask(taskClass)
      client.waitForCompletion()
    } finally {
      client.stop(0)
    }
  }

  @throws[Exception]
  override
  def submit(conf: Configuration): Unit = {
    val actType = conf.get(AngelConf.ANGEL_ACTION_TYPE)
    actType match {
      case MLConf.ANGEL_ML_TRAIN | MLConf.ANGEL_ML_INC_TRAIN  =>
        train(conf)
      case MLConf.ANGEL_ML_PREDICT =>
        predict(conf)
      case _ =>
        println("Error action type, should be train or predict or inctrain")
        System.exit(1)
    }
  }

}
