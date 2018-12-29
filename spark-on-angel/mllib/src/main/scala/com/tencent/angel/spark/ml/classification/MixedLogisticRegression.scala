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

package com.tencent.angel.spark.ml.classification

import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.network.layers.Layer
import com.tencent.angel.ml.core.network.layers.join.DotPooling
import com.tencent.angel.ml.core.network.layers.verge.{SimpleInputLayer, SimpleLossLayer}
import com.tencent.angel.ml.core.network.transfunc.{Sigmoid, Softmax}
import com.tencent.angel.ml.core.optimizer.OptUtils
import com.tencent.angel.ml.core.optimizer.loss.CrossEntropyLoss
import com.tencent.angel.spark.ml.core.GraphModel

class MixedLogisticRegression extends GraphModel {

  val rank: Int = SharedConf.get().getInt(MLConf.ML_MLR_RANK)
  val dataFormat: String = SharedConf.inputDataFormat

  override def network(): Unit = {
    val sigmoid = new SimpleInputLayer("sigmoid_layer", rank, new Sigmoid(),
      OptUtils.getOptimizer(SharedConf.get().get(MLConf.ML_INPUTLAYER_OPTIMIZER, MLConf.DEFAULT_ML_INPUTLAYER_OPTIMIZER)))
    val softmax = new SimpleInputLayer("softmax_layer", rank, new Softmax(),
      OptUtils.getOptimizer(SharedConf.get().get(MLConf.ML_INPUTLAYER_OPTIMIZER, MLConf.DEFAULT_ML_INPUTLAYER_OPTIMIZER)))
    val combined = new DotPooling("dotpooling_layer", 1, Array[Layer](sigmoid, softmax))

    new SimpleLossLayer("simpleLossLayer", combined, new CrossEntropyLoss)
  }

}
