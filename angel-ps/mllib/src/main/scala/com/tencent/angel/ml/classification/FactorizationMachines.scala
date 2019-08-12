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

import com.tencent.angel.ml.core.PSOptimizerProvider
import com.tencent.angel.mlcore.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.core.graphsubmit.AngelModel
import com.tencent.angel.mlcore.network.Identity
import com.tencent.angel.mlcore.network.layers.multiary.SumPooling
import com.tencent.angel.mlcore.network.layers.unary.BiInnerSumCross
import com.tencent.angel.mlcore.network.layers.leaf.{Embedding, SimpleInputLayer}
import com.tencent.angel.mlcore.network.layers.{Layer, LossLayer}
import com.tencent.angel.mlcore.optimizer.loss.LogLoss
import com.tencent.angel.worker.task.TaskContext


class FactorizationMachines(conf: SharedConf, _ctx: TaskContext = null) extends AngelModel(conf, _ctx) {
  val numFields: Int = conf.getInt(MLCoreConf.ML_FIELD_NUM, MLCoreConf.DEFAULT_ML_FIELD_NUM)
  val numFactors: Int = conf.getInt(MLCoreConf.ML_RANK_NUM, MLCoreConf.DEFAULT_ML_RANK_NUM)
  val optProvider = new PSOptimizerProvider(conf)

  override def buildNetwork(): this.type = {
    val inputOptName: String = conf.get(MLCoreConf.ML_INPUTLAYER_OPTIMIZER, MLCoreConf.DEFAULT_ML_INPUTLAYER_OPTIMIZER)
    val wide = new SimpleInputLayer("input", 1, new Identity(), optProvider.getOptimizer(inputOptName))

    val embeddingOptName: String = conf.get(MLCoreConf.ML_EMBEDDING_OPTIMIZER, MLCoreConf.DEFAULT_ML_EMBEDDING_OPTIMIZER)
    val embedding = new Embedding("embedding", numFields * numFactors, numFactors, optProvider.getOptimizer(embeddingOptName))

    val innerSumCross = new BiInnerSumCross("innerSumPooling", embedding)
    val join = new SumPooling("sumPooling", 1, Array[Layer](wide, innerSumCross))

    new LossLayer("simpleLossLayer", join, new LogLoss())

    this
  }
}
