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
import com.tencent.angel.ml.core.network.layers.join.SumPooling
import com.tencent.angel.ml.core.network.layers.linear.FCLayer
import com.tencent.angel.ml.core.network.layers.verge.{Embedding, SimpleInputLayer, SimpleLossLayer}
import com.tencent.angel.ml.core.network.transfunc.{Identity, Relu}
import com.tencent.angel.ml.core.optimizer.Adam
import com.tencent.angel.ml.core.optimizer.loss.LogLoss
import com.tencent.angel.spark.ml.core.GraphModel

class WideAndDeep extends GraphModel {

  val numFields: Int = SharedConf.get().getInt(MLConf.ML_FIELD_NUM)
  val numFactors: Int = SharedConf.get().getInt(MLConf.ML_RANK_NUM)
  val lr: Double = SharedConf.get().getDouble(MLConf.ML_LEARN_RATE)
  val gamma: Double = SharedConf.get().getDouble(MLConf.ML_OPT_ADAM_GAMMA)
  val beta: Double = SharedConf.get().getDouble(MLConf.ML_OPT_ADAM_BETA)


  override def network(): Unit = {
    val optimizer = new Adam(lr, gamma, beta)

    val wide = new SimpleInputLayer("input", 1, new Identity(), optimizer)

    val embedding = new Embedding("embedding", numFields * numFactors, numFactors, optimizer)
    val hidden1 = new FCLayer("hidden1", 80, embedding, new Relu, optimizer)
    val hidden2 = new FCLayer("hidden2", 50, hidden1, new Relu, optimizer)
    val mlpLayer = new FCLayer("hidden3", 1, hidden2, new Identity, optimizer)
    val join = new SumPooling("sumPooling", 1, Array[Layer](wide, mlpLayer))
    new SimpleLossLayer("simpleLossLayer", join, new LogLoss)

  }

}