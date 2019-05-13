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
import com.tencent.angel.ml.core.conf.MLCoreConf
import com.tencent.angel.ml.core.graphsubmit.AngelModel
import com.tencent.angel.ml.core.network.{Identity, TransFunc}
import com.tencent.angel.ml.core.network.layers.join.SumPooling
import com.tencent.angel.ml.core.network.layers.linear.{BiInnerSumCross, FCLayer}
import com.tencent.angel.ml.core.network.layers.verge.{Embedding, SimpleInputLayer}
import com.tencent.angel.ml.core.network.layers.{Layer, LossLayer}
import com.tencent.angel.ml.core.optimizer.loss.LogLoss
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration


class DeepFM(conf: Configuration, _ctx: TaskContext = null) extends AngelModel(conf, _ctx) {
  val numFields: Int = sharedConf.getInt(MLCoreConf.ML_FIELD_NUM, MLCoreConf.DEFAULT_ML_FIELD_NUM)
  val numFactors: Int = sharedConf.getInt(MLCoreConf.ML_RANK_NUM, MLCoreConf.DEFAULT_ML_RANK_NUM)
  val optProvider = new PSOptimizerProvider()

  override def buildNetwork(): this.type = {
    val inputOptName: String = sharedConf.get(MLCoreConf.ML_INPUTLAYER_OPTIMIZER, MLCoreConf.DEFAULT_ML_INPUTLAYER_OPTIMIZER)
    val wide = new SimpleInputLayer("input", 1, new Identity(), optProvider.getOptimizer(inputOptName))

    val embeddingOptName: String = sharedConf.get(MLCoreConf.ML_EMBEDDING_OPTIMIZER, MLCoreConf.DEFAULT_ML_EMBEDDING_OPTIMIZER)
    val embedding = new Embedding("embedding", numFields * numFactors, numFactors, optProvider.getOptimizer(embeddingOptName))

    val innerSumCross = new BiInnerSumCross("innerSumPooling", embedding)

    // outputDim:transFunc:optimizer
    var fcLayer: Layer = innerSumCross
    val fclayerParams = sharedConf.get(MLCoreConf.ML_FCLAYER_PARAMS, MLCoreConf.DEFAULT_ML_FCLAYER_PARAMS)
    fclayerParams.split("|").zipWithIndex.foreach{ case (params: String, idx: Int) =>
      val name = s"fclayer_$idx"
      params.split(":") match {
        case Array(outputDim: String, transFunc: String, optimizer: String) =>
          fcLayer = new FCLayer(name, outputDim.toInt, fcLayer,
            TransFunc.fromString(transFunc), optProvider.getOptimizer(optimizer))
        case Array(outputDim: String, transFunc: String) =>
          fcLayer = new FCLayer(name, outputDim.toInt, fcLayer,
            TransFunc.fromString(transFunc), optProvider.getDefaultOptimizer())
        case Array(outputDim: String) =>
          fcLayer = new FCLayer(name, outputDim.toInt, fcLayer,
            TransFunc.defaultTransFunc(), optProvider.getDefaultOptimizer())
      }
    }

    val join = new SumPooling("sumPooling", 1, Array[Layer](wide, innerSumCross, fcLayer))

    new LossLayer("simpleLossLayer", join, new LogLoss())

    this
  }
}