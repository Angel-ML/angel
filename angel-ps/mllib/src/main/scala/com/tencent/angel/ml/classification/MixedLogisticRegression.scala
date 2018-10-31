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

import com.tencent.angel.ml.core.conf.MLConf
import com.tencent.angel.ml.core.graphsubmit.GraphModel
import com.tencent.angel.ml.core.network.layers.Layer
import com.tencent.angel.ml.core.network.layers.verge.{SimpleInputLayer, SimpleLossLayer}
import com.tencent.angel.ml.core.network.layers.join.DotPooling
import com.tencent.angel.ml.core.network.transfunc.{Sigmoid, Softmax}
import com.tencent.angel.ml.core.optimizer.{OptUtils, Optimizer}
import com.tencent.angel.ml.core.optimizer.loss.{CrossEntropyLoss, LossFunc}
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

/**
  * LR model
  *
  */


class MixedLogisticRegression(conf: Configuration, _ctx: TaskContext = null) extends GraphModel(conf, _ctx) {
  override val lossFunc: LossFunc = new CrossEntropyLoss()
  val rank: Int = sharedConf.getInt(MLConf.ML_MLR_RANK)
  val ipOptName: String = sharedConf.get(MLConf.ML_INPUTLAYER_OPTIMIZER, MLConf.DEFAULT_ML_INPUTLAYER_OPTIMIZER)
  val optimizer: Optimizer = OptUtils.getOptimizer(ipOptName)

  override def buildNetwork(): Unit = {
    val sigmoid = new SimpleInputLayer("sigmoid_layer", rank, new Sigmoid(), optimizer)
    val softmax = new SimpleInputLayer("softmax_layer", rank, new Softmax(), optimizer)
    // name: String, outputDim:Int, blockSize: Int, inputLayers: Array[Layer], conf: Configuration
    val conbined = new DotPooling("dotpooling_layer", 1, Array[Layer](sigmoid, softmax))


    new SimpleLossLayer("simpleLossLayer", conbined, lossFunc)
  }
}