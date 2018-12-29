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


package com.tencent.angel.ml.regression

import com.tencent.angel.ml.core.conf.MLConf
import com.tencent.angel.ml.core.graphsubmit.GraphModel
import com.tencent.angel.ml.core.network.layers.verge.{SimpleLossLayer, SimpleInputLayer}
import com.tencent.angel.ml.core.network.transfunc.Identity
import com.tencent.angel.ml.core.optimizer.OptUtils
import com.tencent.angel.ml.core.optimizer.loss.HuberLoss
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

class RobustRegression(conf: Configuration, _ctx: TaskContext = null)
  extends GraphModel(conf, _ctx) {

  override val lossFunc = new HuberLoss(conf.getDouble(MLConf.ML_ROBUSTREGRESSION_LOSS_DELTA, 1.0))

  override def buildNetwork(): Unit = {
    val input = dataFormat match {
      case "dense" | "component_sparse" => new SimpleInputLayer("input", 1, new Identity(),
        OptUtils.getOptimizer(MLConf.ML_INPUTLAYER_OPTIMIZER))
      case _ => new SimpleInputLayer("input", 1, new Identity(),
        OptUtils.getOptimizer(MLConf.ML_INPUTLAYER_OPTIMIZER))
    }

    new SimpleLossLayer("simpleLossLayer", input, lossFunc)
  }

}