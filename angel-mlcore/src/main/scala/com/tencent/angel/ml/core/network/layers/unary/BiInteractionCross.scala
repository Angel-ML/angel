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


package com.tencent.angel.ml.core.network.layers.unary

import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.utils.MLException
import com.tencent.angel.ml.servingmath2.matrix._
import com.tencent.angel.ml.servingmath2.utils.VectorUtils
import com.tencent.angel.ml.servingmath2.vector.{IntDoubleVector, IntFloatVector, Vector}
import com.tencent.angel.ml.servingmath2.{MFactory, VFactory}
import org.apache.commons.logging.LogFactory


class BiInteractionCross(name: String, outputDim: Int, inputLayer: Layer)(implicit graph: Graph)
  extends LinearLayer(name, outputDim, inputLayer) {
  val LOG = LogFactory.getLog(classOf[BiInteractionCross])

  override protected def doForward(input: Matrix): Matrix = {
    val batchSize = graph.getBatchSize

    input match {
      case mat: RBCompIntDoubleMatrix =>
        val blasMat = MFactory.denseDoubleMatrix(batchSize, outputDim)
        val sum1Vector = VFactory.denseDoubleVector(outputDim)
        val sum2Vector = VFactory.denseDoubleVector(outputDim)
        (0 until batchSize).foreach { row =>
          mat.getRow(row).getPartitions.foreach { vectorOuter =>
            sum1Vector.iadd(vectorOuter)
            sum2Vector.iadd(vectorOuter.mul(vectorOuter))
          }

          blasMat.setRow(row, sum1Vector.imul(sum1Vector).isub(sum2Vector).imul(0.5))
          sum1Vector.clear()
          sum2Vector.clear()
        }
        blasMat
      case mat: RBCompIntFloatMatrix =>
        val blasMat = MFactory.denseFloatMatrix(batchSize, outputDim)
        val sum1Vector = VFactory.denseFloatVector(outputDim)
        val sum2Vector = VFactory.denseFloatVector(outputDim)
        (0 until batchSize).foreach { row =>
          mat.getRow(row).getPartitions.foreach { vectorOuter =>
            sum1Vector.iadd(vectorOuter)
            sum2Vector.iadd(vectorOuter.mul(vectorOuter))
          }

          blasMat.setRow(row, sum1Vector.imul(sum1Vector).isub(sum2Vector).imul(0.5))
          sum1Vector.clear()
          sum2Vector.clear()
        }
        blasMat
      case _ => throw MLException("ERROR! Only Comp Matrix is supported!")
    }
  }

  override protected def doBackward(input: Matrix, gradInput: Matrix): Matrix = {
    conf.valueType() match {
      case "double" =>
        val inputData = input.asInstanceOf[RBCompIntDoubleMatrix]

        val gradRows = inputData.getRows.zipWithIndex.map { case (compVector, idx) =>
          val sumVector = VectorUtils.emptyLike(compVector.getPartitions.head.asInstanceOf[Vector])
          compVector.getPartitions.foreach(comp => sumVector.iadd(comp))

          val grad = gradInput.getRow(idx)

          VFactory.compIntDoubleVector(compVector.getDim, compVector.getPartitions.map { comp =>
            sumVector.sub(comp).imul(grad).asInstanceOf[IntDoubleVector]
          })
        }

        MFactory.rbCompIntDoubleMatrix(gradRows)
      case "float" =>
        val inputData = input.asInstanceOf[RBCompIntFloatMatrix]

        val gradRows = inputData.getRows.zipWithIndex.map { case (compVector, idx) =>
          val sumVector = VectorUtils.emptyLike(compVector.getPartitions.head.asInstanceOf[Vector])
          compVector.getPartitions.foreach(comp => sumVector.iadd(comp))

          val grad = gradInput.getRow(idx)

          VFactory.compIntFloatVector(compVector.getDim, compVector.getPartitions.map { comp =>
            sumVector.sub(comp).imul(grad).asInstanceOf[IntFloatVector]
          })
        }

        MFactory.rbCompIntFloatMatrix(gradRows)
      case _ => throw MLException("Only Dense Data is Support!")
    }
  }
}
