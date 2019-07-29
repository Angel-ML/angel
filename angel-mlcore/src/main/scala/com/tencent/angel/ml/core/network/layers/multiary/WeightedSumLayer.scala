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


package com.tencent.angel.ml.core.network.layers.multiary

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.servingmath2.{MFactory, VFactory}
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.utils.MLException
import com.tencent.angel.ml.servingmath2.matrix._
import com.tencent.angel.ml.servingmath2.vector._
import org.apache.commons.logging.LogFactory
import com.tencent.angel.ml.servingmath2.utils.{MatrixUtils, VectorUtils}


class WeightedSumLayer(name: String, outputDim: Int, inputLayers: Array[Layer])(implicit graph: Graph)
  extends JoinLayer(name, outputDim, inputLayers) {
  private val LOG = LogFactory.getLog(classOf[WeightedSumLayer])

  override protected def doForward(inputs: Map[String, Matrix]): Matrix = {
    val batchSize = graph.placeHolder.getBatchSize

    conf.valueType() match {
      case "double" =>
        val inputsData = inputs.values.toArray
        val weight = MatrixUtils.rbCompDense2Blas(inputsData(0).asInstanceOf[RBCompIntDoubleMatrix])
        val embeddingDim = outputDim
        val multiplicand = inputsData(1).asInstanceOf[RBCompIntDoubleMatrix]
        val output = MFactory.denseDoubleMatrix(batchSize, embeddingDim)

        (0 until batchSize).foreach { row =>
          val vector_output = VFactory.denseDoubleVector(embeddingDim)
          val partitions = multiplicand.getRow(row).getPartitions
          partitions.zipWithIndex.foreach { case (vector_in, index) =>
              vector_output.iadd(vector_in.mul(weight.get(row, index)))
          }
          output.setRow(row, vector_output)
        }
        output

      case "float" =>
        val inputsData = inputs.values.toArray
        val weight = MatrixUtils.rbCompDense2Blas(inputsData(0).asInstanceOf[RBCompIntFloatMatrix])
        val embeddingDim = outputDim
        val multiplicand = inputsData(1).asInstanceOf[RBCompIntFloatMatrix]
        val output = MFactory.denseFloatMatrix(batchSize, embeddingDim)

        (0 until batchSize).foreach { row =>
          val vector_output = VFactory.denseFloatVector(embeddingDim)
          val partitions = multiplicand.getRow(row).getPartitions
          partitions.zipWithIndex.foreach { case (vector_in, index) =>
            vector_output.iadd(vector_in.mul(weight.get(row, index)))
          }
          output.setRow(row, vector_output)
        }
        output
      case _ => throw MLException("Only Dense Matrix is Supported!")

    }
  }


  override protected def doBackward(inputs: Map[String, Matrix], gradInput: Matrix): Map[String, Matrix] = {
    val batchSize = graph.placeHolder.getBatchSize

    conf.valueType() match {
      case "double" =>
        val inputsData = inputs.values.toArray
        val inputsName = inputs.keys.toArray
        val weight = inputsData(0) match {
          case mat: RBCompIntDoubleMatrix =>
            MatrixUtils.rbCompDense2Blas(mat)
          case mat: BlasDoubleMatrix =>
            mat
          case _ => throw MLException("Only BlasMatrix is allowed.")
        }
        val biInteractedNum = weight.getNumCols
        val multiplicand = inputsData(1).asInstanceOf[RBCompIntDoubleMatrix]
        val gradInputData = gradInput.asInstanceOf[BlasDoubleMatrix]
        val weightGrad = MatrixUtils.emptyLike(weight).asInstanceOf[BlasDoubleMatrix]

        (0 until batchSize).foreach { row =>
          val gradVector = gradInputData.getRow(row).asInstanceOf[IntDoubleVector]
          val partitions = multiplicand.getRow(row).getPartitions
          partitions.zipWithIndex.foreach { case (vector_in, index) =>
            weightGrad.set(row, index, vector_in.mul(gradVector).sum)
          }
        }

        val compMat = new Array[CompIntDoubleVector](batchSize)
        (0 until batchSize).foreach { row =>
          val gradVector = gradInputData.getRow(row)
          val compVector = new Array[IntDoubleVector](biInteractedNum)
          (0 until biInteractedNum).foreach { col =>
            val singleWeight = weight.get(row, col)
            compVector(col) = gradVector.mul(singleWeight).asInstanceOf[IntDoubleVector]
          }
          compMat(row) = VFactory.compIntDoubleVector(multiplicand.getRow(row).getDim, compVector)
        }
        val multiplicandGrad = MFactory.rbCompIntDoubleMatrix(compMat)

        Map(inputsName(0) -> weightGrad, inputsName(1) -> multiplicandGrad)

      case "float" =>
        val inputsData = inputs.values.toArray
        val inputsName = inputs.keys.toArray
        val weight = inputsData(0) match {
          case mat: RBCompIntFloatMatrix =>
            MatrixUtils.rbCompDense2Blas(mat)
          case mat: BlasFloatMatrix =>
            mat
          case _ => throw MLException("Only BlasMatrix is allowed.")
        }
        val biInteractedNum = weight.getNumCols
        val multiplicand = inputsData(1).asInstanceOf[RBCompIntFloatMatrix]
        val gradInputData = gradInput.asInstanceOf[BlasFloatMatrix]
        val weightGrad = MatrixUtils.emptyLike(weight).asInstanceOf[BlasFloatMatrix]

        (0 until batchSize).foreach { row =>
          val gradVector = gradInputData.getRow(row).asInstanceOf[IntFloatVector]
          val partitions = multiplicand.getRow(row).getPartitions
          partitions.zipWithIndex.foreach { case (vector_in, index) =>
            weightGrad.set(row, index, vector_in.mul(gradVector).sum.asInstanceOf[Float])
          }
        }

        val compMat = new Array[CompIntFloatVector](batchSize)
        (0 until batchSize).foreach { row =>
          val gradVector = gradInputData.getRow(row)
          val compVector = new Array[IntFloatVector](biInteractedNum)
          (0 until biInteractedNum).foreach { col =>
            val singleWeight = weight.get(row, col)
            compVector(col) = gradVector.mul(singleWeight).asInstanceOf[IntFloatVector]
          }
          compMat(row) = VFactory.compIntFloatVector(multiplicand.getRow(row).getDim, compVector)
        }
        val multiplicandGrad = MFactory.rbCompIntFloatMatrix(compMat)

        Map(inputsName(0) -> weightGrad, inputsName(1) -> multiplicandGrad)
    }
  }

  override def toString: String = {
    s"WeightedSumLayer name=$name outputDim=$outputDim"
  }
}
