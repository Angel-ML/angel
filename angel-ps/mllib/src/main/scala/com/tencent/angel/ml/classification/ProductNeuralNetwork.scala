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
import com.tencent.angel.ml.core.network.layers.verge.{Embedding, SimpleLossLayer, SimpleInputLayer}
import com.tencent.angel.ml.core.network.layers.join.{ConcatLayer, SumPooling}
import com.tencent.angel.ml.core.network.layers.linear.BiInnerCross
import com.tencent.angel.ml.core.network.transfunc.Identity
import com.tencent.angel.ml.core.optimizer.loss.{LogLoss, LossFunc}
import com.tencent.angel.ml.core.utils.paramsutils.{EmbeddingParams, JsonUtils}
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration

class ProductNeuralNetwork(conf: Configuration, _ctx: TaskContext = null) extends GraphModel(conf, _ctx) {
  val numFields: Int = sharedConf.getInt(MLConf.ML_FIELD_NUM, MLConf.DEFAULT_ML_FIELD_NUM)

  override def lossFunc: LossFunc = new LogLoss()

  override def buildNetwork(): Unit = {
    ensureJsonAst()

    val wide = new SimpleInputLayer("input", 1, new Identity(),
      JsonUtils.getOptimizerByLayerType(jsonAst, "SparseInputLayer"))

    val embeddingParams = JsonUtils.getLayerParamsByLayerType(jsonAst, "Embedding")
      .asInstanceOf[EmbeddingParams]
    val embedding = new Embedding("embedding", embeddingParams.outputDim, embeddingParams.numFactors,
      embeddingParams.optimizer.build()
    )

    val crossOutputDim = numFields * (numFields - 1) / 2
    val innerCross = new BiInnerCross("innerPooling", crossOutputDim, embedding)

    val concatOutputDim = embeddingParams.outputDim + crossOutputDim
    val concatLayer = new ConcatLayer("concatMatrix", concatOutputDim, Array[Layer](embedding, innerCross))

    val hiddenLayers = JsonUtils.getFCLayer(jsonAst, concatLayer)

    val join = new SumPooling("sumPooling", 1, Array[Layer](wide, hiddenLayers))

    new SimpleLossLayer("simpleLossLayer", join,lossFunc)
  }
}
