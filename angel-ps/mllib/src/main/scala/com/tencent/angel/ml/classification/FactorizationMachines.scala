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

import com.tencent.angel.ml.core.conf.MLCoreConf
import com.tencent.angel.ml.core.graphsubmit.AngelModel
import com.tencent.angel.ml.core.network.Identity
import com.tencent.angel.ml.core.network.layers.join.SumPooling
import com.tencent.angel.ml.core.network.layers.linear.BiInnerSumCross
import com.tencent.angel.ml.core.network.layers.verge.{Embedding, SimpleInputLayer}
import com.tencent.angel.ml.core.network.layers.{Layer, LossLayer}
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.optimizer.loss.LogLoss
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration


class FactorizationMachines(conf: Configuration, _ctx: TaskContext = null) extends AngelModel(conf, _ctx) {
  val numFields: Int = sharedConf.getInt(MLCoreConf.ML_FIELD_NUM, MLCoreConf.DEFAULT_ML_FIELD_NUM)
  val numFactors: Int = sharedConf.getInt(MLCoreConf.ML_RANK_NUM, MLCoreConf.DEFAULT_ML_RANK_NUM)
  val ipOptName: String = sharedConf.get(MLCoreConf.ML_INPUTLAYER_OPTIMIZER, MLCoreConf.DEFAULT_ML_INPUTLAYER_OPTIMIZER)
  val optimizer: Optimizer = OptUtils.getOptimizer(ipOptName)

  override def buildNetwork(): Unit = {

    val wide = new SimpleInputLayer("input", 1, new Identity(), optimizer)
    val embedding = new Embedding("embedding", numFields * numFactors, numFactors,
      OptUtils.getOptimizer(sharedConf.get(MLCoreConf.ML_EMBEDDING_OPTIMIZER, MLCoreConf.DEFAULT_ML_EMBEDDING_OPTIMIZER)))
    val innerSumCross = new BiInnerSumCross("innerSumPooling", embedding)
    val join = new SumPooling("sumPooling", 1, Array[Layer](wide, innerSumCross))

    new LossLayer("simpleLossLayer", join, new LogLoss())
  }
}
