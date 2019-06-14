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


package com.tencent.angel.ml.core.network.layers.leaf


import com.tencent.angel.ml.core.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.network.{Graph, TransFunc}
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.{LayerKeys, OptUtils}
import com.tencent.angel.ml.core.variable._
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import org.apache.commons.logging.LogFactory
import org.json4s.JsonAST.JField
import org.json4s.JsonDSL._


class SimpleInputLayer(name: String,
                       outputDim: Int,
                       transFunc: TransFunc,
                       override val optimizer: Optimizer)(implicit graph: Graph)
  extends InputLayer(name, outputDim) with Trainable with Serializable {
  graph.addTrainableLayer(this)

  private val LOG = LogFactory.getLog(classOf[SimpleInputLayer])

  private val formatClassName = SharedConf.get().getString(
    MLCoreConf.ML_SIMPLEINPUTLAYER_MATRIX_OUTPUT_FORMAT,
    MLCoreConf.DEFAULT_ML_SIMPLEINPUTLAYER_MATRIX_OUTPUT_FORMAT)
  private val weight: MatVariable = provider.getMatVariable(s"${name}_weight", outputDim,
    SharedConf.indexRange, optimizer, formatClassName, allowPullWithIndex = true)
  private val bias: VecVariable = provider.getVecVariable(s"${name}_bias", outputDim,
    null, formatClassName, allowPullWithIndex = false)

  override protected def doForward(input: Matrix): Matrix = {
    // the input can be both dense and sparse
    // if the input is sparse, then weight can bo either sparse or dense
    // if the input is dense, then weight is dense
    val net = Ufuncs.dot(input, false, weight, true).add(bias)
    transFunc(net)
  }

  override protected def doBackward(input: Matrix, gradInput: Matrix): Unit = {
    val transBack = transFunc.calGrad(forward(), gradInput)
    // the input can be both dense and sparse, but transBack is dense
    val gradWeight: Matrix = Ufuncs.dot(transBack, true, input, false)
      .imul(graph.normalFactor)
    variableManager.putSlot(weight.asInstanceOf[Variable], gradWeight)

    val gradBias = OptUtils.wrapVector2Matrix(transBack.sum(0).imul(graph.normalFactor))
    variableManager.putSlot(bias.asInstanceOf[Variable], gradBias)
  }

  override def toString: String = {
    s"SimpleInputLayer name=$name outputDim=$outputDim optimizer=$optimizer"
  }

  override def toJson: JField = {
    val layerJson = (LayerKeys.typeKey -> s"${this.getClass.getSimpleName}") ~
      (LayerKeys.outputDimKey -> outputDim) ~
      (LayerKeys.transFuncKey -> transFunc.toJson) ~
      (LayerKeys.optimizerKey -> optimizer.toJson)

    JField(name, layerJson)
  }
}
