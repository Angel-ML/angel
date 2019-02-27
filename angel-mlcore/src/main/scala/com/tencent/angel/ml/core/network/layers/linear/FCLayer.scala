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


import com.tencent.angel.ml.core.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.network.{Graph, TransFunc}
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.{LayerKeys, MLException, OptUtils}
import com.tencent.angel.ml.core.variable.{MatVariable, Variable, VecVariable}
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.utils.MatrixUtils
import org.apache.commons.logging.LogFactory
import org.json4s.JsonAST.{JField, JString}
import org.json4s.JsonDSL._

import scala.language.implicitConversions

class FCLayer(name: String, outputDim: Int, inputLayer: Layer, transFunc: TransFunc, override val optimizer: Optimizer
             )(implicit graph: Graph) extends LinearLayer(name, outputDim, inputLayer) with Trainable {
  graph.addTrainableLayer(this)
  private val LOG = LogFactory.getLog(classOf[FCLayer])

  private val formatClassName = SharedConf.get().getString(
    MLCoreConf.ML_FCLAYER_MATRIX_OUTPUT_FORMAT,
    MLCoreConf.DEFAULT_ML_FCLAYER_MATRIX_OUTPUT_FORMAT)
  private val weight: MatVariable = graph.provider.getMatVariable(s"${name}_weight", outputDim,
    inputLayer.outputDim, optimizer, formatClassName, allowPullWithIndex = false)
  private val bias: VecVariable = graph.provider.getVecVariable(s"${name}_bias", outputDim,
    null, formatClassName, allowPullWithIndex = false)

  @transient private var middleCache: Matrix = _
  @transient private var subDim: Int = -1

  override protected def doForward(input: Matrix): Matrix = {
    val inputNew = input match {
      case mat: RBCompIntDoubleMatrix =>
        // for Double embedding layer
        middleCache = MatrixUtils.rbCompDense2Blas(mat)
        subDim = mat.getSubDim
        middleCache
      case mat: RBCompIntFloatMatrix =>
        // for Double embedding layer
        middleCache = MatrixUtils.rbCompDense2Blas(mat)
        subDim = mat.getSubDim
        middleCache
      case mat: BlasMatrix =>
        // other layers, but the input layer, their out put is Blas
        mat
      case _ => throw MLException("Only BlasMatrix is allowed!")
    }

    // both inputNew and weight are Blas
    val net = Ufuncs.dot(inputNew, false, weight, true).add(bias)
    transFunc(net)
  }

  override protected def doBackward(input: Matrix, gradInput: Matrix): Matrix = {
    // 1. calculate backward
    val transBack = transFunc.calGrad(forward(), gradInput)
    // both transBack and weight are Blas
    val backwardTemp: Matrix = Ufuncs.dot(transBack, false, weight, false)

    // 2. calculate gradient
    val (backwardValue, lastOutput) = if (middleCache != null) {
      val backwardValue: Matrix = backwardTemp match {
        case mat: BlasDoubleMatrix => MatrixUtils.blas2RBCompDense(mat, subDim)
        case mat: BlasFloatMatrix => MatrixUtils.blas2RBCompDense(mat, subDim)
      }
      backwardValue -> middleCache
    } else {
      backwardTemp -> input
    }

    graph.put2Cache(backwardKey, backwardValue)

    // both transBack and lastOutput are Blas
    val gradWeight = Ufuncs.dot(transBack, true, lastOutput, false)
      .imul(graph.normalFactor)
    variableManager.putSlot(weight.asInstanceOf[Variable], gradWeight)

    val gradBias = OptUtils.wrapVector2Matrix(transBack.sum(0).imul(graph.normalFactor))
    variableManager.putSlot(bias.asInstanceOf[Variable], gradBias)

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
