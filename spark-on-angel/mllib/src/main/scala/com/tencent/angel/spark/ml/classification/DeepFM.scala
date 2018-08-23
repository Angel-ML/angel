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
import com.tencent.angel.ml.core.network.layers.edge.inputlayer.{Embedding, SparseInputLayer}
import com.tencent.angel.ml.core.network.layers.edge.losslayer.SimpleLossLayer
import com.tencent.angel.ml.core.network.layers.join.SumPooling
import com.tencent.angel.ml.core.network.layers.linear.{BiInnerSumCross, FCLayer}
import com.tencent.angel.ml.core.network.transfunc.{Identity, Relu}
import com.tencent.angel.ml.core.optimizer.Adam
import com.tencent.angel.ml.core.optimizer.loss.LogLoss
import com.tencent.angel.spark.ml.core.GraphModel

class DeepFM extends GraphModel {

  val numFields: Int = SharedConf.get().getInt(MLConf.ML_FIELD_NUM)
  val numFactors: Int = SharedConf.get().getInt(MLConf.ML_RANK_NUM)
  val lr: Double = SharedConf.get().getDouble(MLConf.ML_LEARN_RATE)

  override
  def network(): Unit = {
    val wide = new SparseInputLayer("input", 1, new Identity(), new Adam(lr))
    val embedding = new Embedding("embedding", numFields * numFactors, numFactors, new Adam(lr))
    val innerSumCross = new BiInnerSumCross("innerSumPooling", embedding)
    val hidden1 = new FCLayer("hidden1", 80, embedding, new Relu, new Adam(lr))
    val hidden2 = new FCLayer("hidden2", 50, hidden1, new Relu, new Adam(lr))
    val mlpLayer = new FCLayer("hidden3", 1, hidden2, new Identity, new Adam(lr))
    val join = new SumPooling("sumPooling", 1, Array[Layer](wide, innerSumCross, mlpLayer))
    new SimpleLossLayer("simpleLossLayer", join, new LogLoss)
  }
}
