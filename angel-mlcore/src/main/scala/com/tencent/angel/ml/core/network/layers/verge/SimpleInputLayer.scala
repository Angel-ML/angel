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


package com.tencent.angel.ml.core.network.layers.verge


import com.tencent.angel.ml.core.network.{Graph, TransFunc}
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.network.variable._
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.{LayerKeys, MLException, OptUtils}
import com.tencent.angel.ml.math2.MFactory
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

  private val weight: MatVariable = graph.provider.getMatVariable(s"${name}_weight", outputDim,
    graph.indexRange, optimizer, allowPullWithIndex = true)
  private val bias: VecVariable = graph.provider.getVecVariable(s"${name}_bias", outputDim,
    null, allowPullWithIndex = false)

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

    gradWeight.imul(graph.normalFactor)
    graph.putGradient(weight.asInstanceOf[Variable], gradWeight)

    val gradBias = OptUtils.wrapVector2Matrix(gradWeight.average(1))

    graph.putGradient(bias.asInstanceOf[Variable], gradBias)
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
