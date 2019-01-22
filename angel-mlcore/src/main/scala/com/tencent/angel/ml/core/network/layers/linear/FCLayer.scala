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


package com.tencent.angel.ml.core.network.layers.linear


import com.tencent.angel.ml.core.network.TransFunc
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.utils.MatrixUtils
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.network.variable.{MatVariable, Variable, VecVariable}
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.{LayerKeys, MLException, MathUtils}
import org.apache.commons.logging.LogFactory
import org.json4s.JsonAST.{JField, JString}
import org.json4s.JsonDSL._

import scala.language.implicitConversions

class FCLayer(name: String, outputDim: Int, inputLayer: Layer, transFunc: TransFunc, override val optimizer: Optimizer
             )(implicit graph: Graph) extends LinearLayer(name, outputDim, inputLayer) with Trainable {
  graph.addTrainableLayer(this)
  private val LOG = LogFactory.getLog(classOf[FCLayer])

  private val weight: MatVariable = graph.provider.getMatVariable(s"${name}_weight", outputDim,
    inputLayer.outputDim, optimizer, withInput = false)
  private val bias: VecVariable = graph.provider.getVecVariable(s"${name}_bias", outputDim,
    null, withInput = false)

  @transient private var middleCache: Matrix = _

  override protected def doForward(input: Matrix): Matrix = {
    val inputNew = input match {
      case mat: RBCompIntDoubleMatrix =>
        middleCache = MatrixUtils.rbCompDense2Blas(mat)
        middleCache
      case mat: RBCompIntFloatMatrix =>
        middleCache = MatrixUtils.rbCompDense2Blas(mat)
        middleCache
      case mat: BlasMatrix => mat
      case _ => throw MLException("Only BlasMatrix is allowed!")
    }

    val net = MathUtils.rowDot(inputNew, weight).add(bias)
    transFunc(net)
  }

  override protected def doBackward(input: Matrix, gradInput: Matrix): Matrix = {
    // 1. calculate backward
    val transBack = transFunc.calGrad(forward(), gradInput)
    val backwardValue = Ufuncs.dot(transBack, false, weight, false)
    graph.put2Cache(backwardKey, backwardValue)

    // 2. calculate gradient
    val lastOutput = if (middleCache!= null) {
      middleCache
    } else {
      input
    }

    val gradWeight = Ufuncs.dot(transBack, false, lastOutput, true)
    gradWeight.imul(graph.normalFactor)

    graph.putGradient(weight.asInstanceOf[Variable], gradWeight)

    graph.putGradient(bias.asInstanceOf[Variable],
      MathUtils.wrapVector2Matrix(gradWeight.average(0))
    )

    backwardValue
  }

  override def toString: String = {
    s"FCLayer name=$name outputDim=$outputDim optimizer=$optimizer transFunc=${transFunc.getClass.getSimpleName}"
  }

  override def toJson: JField = {
    val layerJson = (LayerKeys.typeKey -> s"${this.getClass.getSimpleName}") ~
      (LayerKeys.outputDimKey -> outputDim) ~
      (LayerKeys.inputLayerKey, JString(inputLayer.name)) ~
      (LayerKeys.transFuncKey -> transFunc.toJson) ~
      (LayerKeys.optimizerKey -> optimizer.toJson)

    JField(name, layerJson)
  }
}
