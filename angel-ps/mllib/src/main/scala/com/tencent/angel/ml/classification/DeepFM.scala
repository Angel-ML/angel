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

import com.tencent.angel.ml.core.graphsubmit.GraphModel
import com.tencent.angel.ml.core.network.layers.Layer
import com.tencent.angel.ml.core.network.layers.edge.inputlayer.{Embedding, SparseInputLayer}
import com.tencent.angel.ml.core.network.layers.edge.losslayer.SimpleLossLayer
import com.tencent.angel.ml.core.network.layers.join.SumPooling
import com.tencent.angel.ml.core.network.layers.linear.BiInnerSumCross
import com.tencent.angel.ml.core.network.transfunc._
import com.tencent.angel.ml.core.optimizer.loss.{LogLoss, LossFunc}
import com.tencent.angel.ml.core.utils.paramsutils.{EmbeddingParams, JsonUtils}
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.conf.Configuration


class DeepFM(conf: Configuration, _ctx: TaskContext = null) extends GraphModel(conf, _ctx) {
  override val lossFunc: LossFunc = new LogLoss()

  override def buildNetwork(): Unit = {
    ensureJsonAst()

    val wide = new SparseInputLayer("input", 1, new Identity(),
      JsonUtils.getOptimizerByLayerType(jsonAst, "SparseInputLayer")
    )

    val embeddingParams = JsonUtils.getLayerParamsByLayerType(jsonAst, "Embedding")
      .asInstanceOf[EmbeddingParams]
    val embedding = new Embedding("embedding", embeddingParams.outputDim,
      embeddingParams.numFactors, embeddingParams.optimizer.build()
    )

    val innerSumCross = new BiInnerSumCross("innerSumPooling", embedding)

    val mlpLayer = JsonUtils.getFCLayer(jsonAst, embedding)

    val join = new SumPooling("sumPooling", 1, Array[Layer](wide, innerSumCross, mlpLayer))

    new SimpleLossLayer("simpleLossLayer", join, lossFunc)
  }
}