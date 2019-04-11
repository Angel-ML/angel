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
import com.tencent.angel.ml.core.network.layers.join.SumPooling
import com.tencent.angel.ml.core.network.layers.linear.BiInnerSumCross
import com.tencent.angel.ml.core.network.layers.verge.{Embedding, SimpleInputLayer, SimpleLossLayer}
import com.tencent.angel.ml.core.network.transfunc.Identity
import com.tencent.angel.ml.core.optimizer.Adam
import com.tencent.angel.ml.core.optimizer.loss.LogLoss
import com.tencent.angel.spark.ml.core.GraphModel

class FactorizationMachine extends GraphModel {

  val numField: Int = conf.getInt(MLConf.ML_FIELD_NUM)
  val numFactor: Int = conf.getInt(MLConf.ML_RANK_NUM)
  val lr: Double = conf.getDouble(MLConf.ML_LEARN_RATE)
  val gamma: Double = SharedConf.get().getDouble(MLConf.ML_OPT_ADAM_GAMMA)
  val beta: Double = SharedConf.get().getDouble(MLConf.ML_OPT_ADAM_BETA)

  override
  def network(): Unit = {
    val optimizer = new Adam(lr, gamma, beta)
    val wide = new SimpleInputLayer("wide", 1, new Identity(), optimizer)
    val embedding = new Embedding("embedding", numField * numFactor, numFactor, optimizer)
    val crossFeature = new BiInnerSumCross("innerSumPooling", embedding)
    val sum = new SumPooling("sum", 1, Array(wide, crossFeature))
    new SimpleLossLayer("simpleLossLayer", sum, new LogLoss)
  }
}
