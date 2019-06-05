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


package com.tencent.angel.ml.core.network.layers.join

import com.tencent.angel.ml.core.conf.{MLCoreConf, SharedConf}
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.{MLException, OptUtils}
import com.tencent.angel.ml.core.variable.{MatVariable, Variable, VecVariable}
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.vector._
import org.apache.commons.logging.LogFactory
import com.tencent.angel.ml.math2.utils.{MatrixUtils, VectorUtils}


class CrossLayer(name: String, outputDim: Int, inputLayers: Array[Layer], override val optimizer: Optimizer)(implicit graph: Graph)
  extends JoinLayer(name, outputDim, inputLayers) with Trainable {
  private val LOG = LogFactory.getLog(classOf[CrossLayer])

  private val formatClassName = SharedConf.get().getString(
    MLCoreConf.ML_FCLAYER_MATRIX_OUTPUT_FORMAT,
    MLCoreConf.DEFAULT_ML_FCLAYER_MATRIX_OUTPUT_FORMAT)
  private val weight: VecVariable = graph.provider.getVecVariable(s"${name}_weight", outputDim,
    optimizer, formatClassName, allowPullWithIndex = false)
  private val bias: VecVariable = graph.provider.getVecVariable(s"${name}_bias", outputDim,
    null, formatClassName, allowPullWithIndex = false)

  @transient private var middleCache_1: Matrix = _
  @transient private var middleCache_0: Matrix = _
  @transient private var subDim_1: Int = -1
  @transient private var subDim_0: Int = -1

  override protected def doForward(inputs: Map[String, Matrix]): Matrix = {

    val inputsData = inputs.values.toArray
    val input_0 = inputsData(0) match {
      case mat: RBCompIntDoubleMatrix =>
        middleCache_0 = MatrixUtils.rbCompDense2Blas(mat)
        subDim_0 = mat.getSubDim
        middleCache_0
      case mat: RBCompIntFloatMatrix =>
        middleCache_0 = MatrixUtils.rbCompDense2Blas(mat)
        subDim_0 = mat.getSubDim
        middleCache_0
      case mat: BlasMatrix =>
        mat
      case _ => throw MLException("Only BlasMatrix is allowed!")
    }
    val input_1 = inputsData(1) match {
      case mat: RBCompIntDoubleMatrix =>
        middleCache_1 = MatrixUtils.rbCompDense2Blas(mat)
        subDim_1 = mat.getSubDim
        middleCache_1
      case mat: RBCompIntFloatMatrix =>
        middleCache_1 = MatrixUtils.rbCompDense2Blas(mat)
        subDim_1 = mat.getSubDim
        middleCache_1
      case mat: BlasMatrix =>
        mat
      case _ => throw MLException("Only BlasMatrix is allowed!")
    }
    val temp = Ufuncs.dot(input_1,weight)
    val net = input_0.mul(temp).add(bias).add(input_1)
    net
  }

  override protected def doBackward(inputs: Map[String, Matrix], gradInput: Matrix): Map[String, Matrix] = {
    val inputsData = inputs.values.toArray
    val inputsName = inputs.keys.toArray

    val (inputsData_0, inputsData_1) = if (middleCache_0 != null && middleCache_1 != null) {
      middleCache_0 -> middleCache_1
    }else if (middleCache_0 != null && middleCache_1 == null) {
      middleCache_0 -> inputsData(1)
    }else if (middleCache_0 == null && middleCache_1 != null) {
      inputsData(0) -> middleCache_1
    }else {
      inputsData(0) -> inputsData(1)
    }

    val gradOutput_0_ = gradInput.imul(Ufuncs.dot(inputsData_1, weight))
    val gradOutput_0 = if (middleCache_1 != null) {
      val temp = gradOutput_0_ match {
        case mat: BlasDoubleMatrix => MatrixUtils.blas2RBCompDense(mat, subDim_0)
        case mat: BlasFloatMatrix => MatrixUtils.blas2RBCompDense(mat, subDim_0)
      }
      temp
    } else {
      gradOutput_0_
    }
    val gradOutput_1_ = Ufuncs.mul(inputsData_0, weight, false).add(1.0).mul(gradInput)
    val gradOutput_1 = if (middleCache_1 != null) {
      val temp = gradOutput_1_ match {
        case mat: BlasDoubleMatrix => MatrixUtils.blas2RBCompDense(mat, subDim_1)
        case mat: BlasFloatMatrix => MatrixUtils.blas2RBCompDense(mat, subDim_1)
      }
      temp
    } else {
      gradOutput_1_
    }

    val gradWeight = Ufuncs.mul(inputsData_0, inputsData_1).mul(gradInput).mul(graph.normalFactor)
    variableManager.putSlot(weight.asInstanceOf[Variable], gradWeight)

    val gradBias = OptUtils.wrapVector2Matrix(gradInput.sum(0).mul(graph.normalFactor))
    variableManager.putSlot(bias.asInstanceOf[Variable], gradBias)

    Map(inputsName(0) -> gradOutput_0, inputsName(1) -> gradOutput_1)
  }

  override def toString: String = {
    s"CrossLayer name=$name outputDim=$outputDim"
  }
}
