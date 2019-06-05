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
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.utils.{MatrixUtils, VectorUtils}
import com.tencent.angel.ml.math2.vector.{CompIntDoubleVector, IntDoubleVector}
import org.apache.commons.logging.LogFactory
import org.json4s.JsonAST.{JField, JString}
import org.json4s.JsonDSL._
import com.tencent.angel.ml.math2.utils.{MatrixUtils, VectorUtils}

import scala.language.implicitConversions

class ParamSharedFC(name: String, outputDim: Int, inputLayer: Layer, transFunc: TransFunc, weightDim: Int, override val optimizer: Optimizer
             )(implicit graph: Graph) extends LinearLayer(name, outputDim, inputLayer) with Trainable {
  graph.addTrainableLayer(this)
  private val LOG = LogFactory.getLog(classOf[ParamSharedFC])

  private val formatClassName = SharedConf.get().getString(
    MLCoreConf.ML_FCLAYER_MATRIX_OUTPUT_FORMAT,
    MLCoreConf.DEFAULT_ML_FCLAYER_MATRIX_OUTPUT_FORMAT)

  private val weightDim_ = inputLayer.outputDim * weightDim / outputDim

  private val weight: MatVariable = graph.provider.getMatVariable(s"${name}_weight", weightDim,
    weightDim_, optimizer, formatClassName, allowPullWithIndex = false)
  private val bias: VecVariable = graph.provider.getVecVariable(s"${name}_bias", weightDim,
    null, formatClassName, allowPullWithIndex = false)

  @transient private var middleCache: Matrix = _
  @transient private var subDim: Int = -1

  override protected def doForward(input: Matrix): Matrix = {
    val batchSize = graph.placeHolder.getBatchSize

    val inputNew = input match {
      case mat: BlasDoubleMatrix =>
        // for Double embedding layer
        middleCache = MatrixUtils.blas2RBCompDense(mat, weightDim_)
        middleCache
      case mat: BlasFloatMatrix =>
        // for Double embedding layer
        middleCache = MatrixUtils.blas2RBCompDense(mat, weightDim_)
        middleCache
      case mat: RBCompIntDoubleMatrix =>
        mat
      case mat: RBCompIntFloatMatrix =>
        mat
      case _ => throw MLException("Only CompMatrix is allowed!")
    }

    SharedConf.valueType() match {
      case "double" =>
        val inputData = inputNew.asInstanceOf[RBCompIntDoubleMatrix]
        val outputMatrix = new Array[CompIntDoubleVector](batchSize)
        (0 until batchSize).foreach { row =>
          val partitionLength = inputData.getRow(row).getNumPartitions
          val compVector = new Array[IntDoubleVector](partitionLength)
          val partitions = inputData.getRow(row).getPartitions
          partitions.zipWithIndex.foreach { case (vector_in, index) =>
            compVector(index) = Ufuncs.dot(weight, vector_in).add(bias).asInstanceOf[IntDoubleVector]
          }
          outputMatrix(row) = VFactory.compIntDoubleVector(partitionLength * weightDim, compVector)
        }
        transFunc(MFactory.rbCompIntDoubleMatrix(outputMatrix))
      case "float" =>
        val inputData = input.asInstanceOf[RBCompIntFloatMatrix]
        val outputMatrix = new Array[CompIntFloatVector](batchSize)
        (0 until batchSize).foreach { row =>
          val partitionLength = inputData.getRow(row).getNumPartitions
          val compVector = new Array[IntFloatVector](partitionLength)
          val partitions = inputData.getRow(row).getPartitions
          partitions.zipWithIndex.foreach { case (vector_in, index) =>
            compVector(index) = Ufuncs.dot(weight, vector_in).add(bias).asInstanceOf[IntFloatVector]
          }
          outputMatrix(row) = VFactory.compIntFloatVector(partitionLength * weightDim, compVector)
        }
        transFunc(MFactory.rbCompIntFloatMatrix(outputMatrix))

      case _ => throw MLException("Only Comp Matrix is Supported!")
    }
  }

  override protected def doBackward(input: Matrix, gradInput: Matrix): Matrix = {
    val batchSize = graph.placeHolder.getBatchSize
    // 1. calculate backward
    val transBack = transFunc.calGrad(transClassForInput(forward(), gradInput), gradInput)

    SharedConf.valueType() match {
      case "double" =>
        val inputData = input.asInstanceOf[RBCompIntDoubleMatrix]
        val gradInputData = transBack match {
          case mat: RBCompIntDoubleMatrix => mat
          case mat: BlasDoubleMatrix =>
            MatrixUtils.blas2RBCompDense(mat, weightDim)
          case _ => throw MLException("Only CompMatrix is allowed.")
        }
        var weightGradOutput = MFactory.denseDoubleMatrix(weightDim, weightDim_)
        val inputGradOutput_ = new Array[CompIntDoubleVector](batchSize)
        (0 until batchSize).foreach { row =>
          val partitionLength = inputData.getRow(row).getNumPartitions
          val transInput = getMatrixFromRow(inputData, row, weightDim_)
          val transGrandInput = getMatrixFromRow(gradInputData, row, weightDim)
          val temp = Ufuncs.dot(transGrandInput, true, transInput, false)
          weightGradOutput = weightGradOutput.add(temp).asInstanceOf[BlasDoubleMatrix]

          val temp_ = Ufuncs.dot(transGrandInput, false, weight, false)
          val compVector = new Array[IntDoubleVector](partitionLength)
          (0 until partitionLength).foreach { x =>
            compVector(x) = temp_.getRow(x).asInstanceOf[IntDoubleVector]
          }
          inputGradOutput_(row) = VFactory.compIntDoubleVector(partitionLength * weightDim_, compVector)
        }
        weightGradOutput.imul(graph.normalFactor)
        variableManager.putSlot(weight.asInstanceOf[Variable], weightGradOutput)

        val biasGradOutput = OptUtils.wrapVector2Matrix(transBack.sum(0).imul(graph.normalFactor))
        variableManager.putSlot(bias.asInstanceOf[Variable], biasGradOutput)

        MFactory.rbCompIntDoubleMatrix(inputGradOutput_)
      case "float" =>
        val inputData = input.asInstanceOf[RBCompIntFloatMatrix]
        val gradInputData = transBack match {
          case mat: RBCompIntFloatMatrix => mat
          case mat: BlasFloatMatrix =>
            MatrixUtils.blas2RBCompDense(mat, weightDim)
          case _ => throw MLException("Only CompMatrix is allowed.")
        }
        var weightGradOutput = MFactory.denseFloatMatrix(weightDim, weightDim_)
        var biasGradOutput = VFactory.denseFloatVector(weightDim)
        val inputGradOutput_ = new Array[CompIntFloatVector](batchSize)

        (0 until batchSize).foreach { row =>
          val partitionLength = inputData.getRow(row).getNumPartitions
          val transInput = getMatrixFromRow(inputData, row, weightDim_)
          val transGrandInput = getMatrixFromRow(gradInputData, row, weightDim)

          val temp = Ufuncs.dot(transGrandInput, true, transInput, false)
          weightGradOutput = weightGradOutput.add(temp).asInstanceOf[BlasFloatMatrix]
          biasGradOutput = biasGradOutput.add(transGrandInput.sum(0)).asInstanceOf[IntFloatVector]

          val temp_ = Ufuncs.dot(transGrandInput, false, weight, false)
          val compVector = new Array[IntFloatVector](partitionLength)
          (0 until partitionLength).foreach { x =>
            compVector(x) = temp_.getRow(x).asInstanceOf[IntFloatVector]
          }
          inputGradOutput_(row) = VFactory.compIntFloatVector(partitionLength * weightDim_, compVector)
        }
        weightGradOutput.imul(graph.normalFactor)
        variableManager.putSlot(weight.asInstanceOf[Variable], weightGradOutput)

        biasGradOutput.imul(graph.normalFactor)
        variableManager.putSlot(bias.asInstanceOf[Variable], OptUtils.wrapVector2Matrix(biasGradOutput))


        MFactory.rbCompIntFloatMatrix(inputGradOutput_)
    }
  }

  override def toString: String = {
    s"FCLayer name=$name outputDim=$outputDim optimizer=$optimizer transFunc=${transFunc.getClass.getSimpleName}"
  }

  override def toJson: JField = {
    val layerJson = (LayerKeys.typeKey -> s"${this.getClass.getSimpleName}") ~
      (LayerKeys.outputDimKey -> outputDim) ~
      (LayerKeys.inputLayerKey, JString(inputLayer.name)) ~
      (LayerKeys.transFuncKey -> transFunc.toJson) ~
      (LayerKeys.optimizerKey -> optimizer.toJson) ~
      (LayerKeys.weightDimKey -> weightDim)

    JField(name, layerJson)
  }

  private def transClassForInput(forwardOut: Matrix, gradInput: Matrix): Matrix = {
    val temp = gradInput match {
      case _: BlasMatrix => forwardOut match {
        case x: RBCompIntFloatMatrix => MatrixUtils.rbCompDense2Blas(x)
        case x: RBCompIntDoubleMatrix => MatrixUtils.rbCompDense2Blas(x)
        case _ => forwardOut
      }
      case _: RBCompIntFloatMatrix => forwardOut match {
        case x: BlasFloatMatrix => MatrixUtils.blas2RBCompDense(x, weightDim_)
        case _ => forwardOut
      }
      case _: RBCompIntDoubleMatrix => forwardOut match {
        case x: BlasDoubleMatrix => MatrixUtils.blas2RBCompDense(x, weightDim_)
        case _ => forwardOut
      }
    }
    temp
  }

  private def getMatrixFromRow(inputData: Matrix, row: Int, subDime: Int): Matrix = {
    val out = inputData match {
      case mat: RBCompIntFloatMatrix =>
        val inputPartitions = mat.getRow(row).getPartitions
        val transInput = MFactory.denseFloatMatrix(inputPartitions.length, subDime)
        inputPartitions.zipWithIndex.foreach { case (vector_in, index) =>
          transInput.setRow(index, vector_in)
        }
        transInput
      case mat: RBCompIntDoubleMatrix =>
        val inputPartitions = mat.getRow(row).getPartitions
        val transInput = MFactory.denseDoubleMatrix(inputPartitions.length, subDime)
        inputPartitions.zipWithIndex.foreach { case (vector_in, index) =>
          transInput.setRow(index, vector_in)
        }
        transInput
    }
    out
  }
}
