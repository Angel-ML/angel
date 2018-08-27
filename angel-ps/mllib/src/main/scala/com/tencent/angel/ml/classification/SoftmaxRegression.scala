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


package com.tencent.angel.ml.classification

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.graphsubmit.GraphModel
import com.tencent.angel.ml.core.network.layers.edge.inputlayer.{DenseInputLayer, SparseInputLayer}
import com.tencent.angel.ml.core.network.layers.edge.losslayer.SoftmaxLossLayer
import com.tencent.angel.ml.core.network.transfunc.Identity
import com.tencent.angel.ml.core.optimizer.OptUtils
import com.tencent.angel.ml.core.optimizer.loss.{LossFunc, SoftmaxLoss}
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

/**
  * LR model
  *
  */

class SoftmaxRegression(conf: Configuration, _ctx: TaskContext = null)
  extends GraphModel(conf, _ctx) {
  val numClass: Int = SharedConf.numClass

  override val lossFunc: LossFunc = new SoftmaxLoss()

  override def buildNetwork(): Unit = {
    val input = dataFormat match {
      case "dense" => new DenseInputLayer("input", numClass, new Identity(),
        OptUtils.getOptimizer(sharedConf.get(MLConf.ML_DENSEINPUTLAYER_OPTIMIZER, MLConf.DEFAULT_ML_DENSEINPUTLAYER_OPTIMIZER)))
      case _ => new SparseInputLayer("input", numClass, new Identity(),
        OptUtils.getOptimizer(sharedConf.get(MLConf.ML_SPARSEINPUTLAYER_OPTIMIZER, MLConf.DEFAULT_ML_SPARSEINPUTLAYER_OPTIMIZER)))
    }
    new SoftmaxLossLayer("softmaxLossLayer", input, lossFunc)
  }

}