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
import com.tencent.angel.ml.core.network.layers.linear.BiInteractionCross
import com.tencent.angel.ml.core.network.layers.verge.{Embedding, SimpleInputLayer, SimpleLossLayer}
import com.tencent.angel.ml.core.network.transfunc.Identity
import com.tencent.angel.ml.core.optimizer.loss.LogLoss
import com.tencent.angel.ml.core.utils.paramsutils.{EmbeddingParams, JsonUtils}
import com.tencent.angel.spark.ml.core.GraphModel

class NeuralFM extends GraphModel {

  val numFields: Int = SharedConf.get().getInt(MLConf.ML_FIELD_NUM, MLConf.DEFAULT_ML_FIELD_NUM)

  override def network(): Unit = {
    ensureJsonAst()

    val wide = new SimpleInputLayer("input", 1, new Identity(),
      JsonUtils.getOptimizerByLayerType(jsonAst, "SparseInputLayer"))

    val embeddingParams = JsonUtils.getLayerParamsByLayerType(jsonAst, "Embedding")
      .asInstanceOf[EmbeddingParams]
    val embedding = new Embedding("embedding", embeddingParams.outputDim, embeddingParams.numFactors,
      embeddingParams.optimizer.build()
    )

    val interactionCross = new BiInteractionCross("BiInteractionCross", embeddingParams.numFactors, embedding)
    val hiddenLayer = JsonUtils.getFCLayer(jsonAst, interactionCross)

    val join = new SumPooling("sumPooling", 1, Array[Layer](wide, hiddenLayer))

    new SimpleLossLayer("simpleLossLayer", join, new LogLoss)
  }

}
