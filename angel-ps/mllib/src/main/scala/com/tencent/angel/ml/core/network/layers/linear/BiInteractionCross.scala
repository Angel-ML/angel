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

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.utils.VectorUtils
import com.tencent.angel.ml.math2.vector.{IntDoubleVector, IntFloatVector, Vector}
import com.tencent.angel.ml.matrix.RowType
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import org.apache.commons.logging.LogFactory


class BiInteractionCross(name: String, outputDim: Int, inputLayer: Layer)(
  implicit graph: AngelGraph) extends LinearLayer(name, outputDim, inputLayer)(graph) {
  val LOG = LogFactory.getLog(classOf[BiInteractionCross])
  val modelType: RowType = SharedConf.denseModelType

  @transient var output: Matrix = _
  @transient var gradOutput: Matrix = _

  override def calOutput(): Matrix = {
    val batchSize = graph.placeHolder.getBatchSize
    status match {
      case STATUS.Null =>
        output = inputLayer.calOutput() match {
          case mat: RBCompIntDoubleMatrix =>
            val blasMat = MFactory.denseDoubleMatrix(batchSize, outputDim)
            val sum1Vector = VFactory.denseDoubleVector(outputDim)
            val sum2Vector = VFactory.denseDoubleVector(outputDim)
            (0 until batchSize).foreach { row =>
              mat.getRow(row).getPartitions.foreach { vectorOuter =>
                sum1Vector.iadd(vectorOuter)
                sum2Vector.iadd(vectorOuter.mul(vectorOuter))
              }

              blasMat.setRow(row, sum1Vector.mul(sum1Vector).isub(sum2Vector).imul(0.5))
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

              blasMat.setRow(row, sum1Vector.mul(sum1Vector).isub(sum2Vector).imul(0.5))
              sum1Vector.clear()
              sum2Vector.clear()
            }
            blasMat
        }
        status = STATUS.Forward
      case _ =>
    }
    output
  }

  override def calGradOutput(): Matrix = {
    status match {
      case STATUS.Forward =>
        val gradTemp = gatherGrad()

        gradOutput = modelType match {
          case RowType.T_DOUBLE_DENSE =>
            val inputData = inputLayer.calOutput().asInstanceOf[RBCompIntDoubleMatrix]

            val gradRows = inputData.getRows.zipWithIndex.map { case (compVector, idx) =>
              val sumVector = VectorUtils.emptyLike(compVector.getPartitions.head.asInstanceOf[Vector])
              compVector.getPartitions.foreach(comp => sumVector.iadd(comp))

              val grad = gradTemp.getRow(idx)

              VFactory.compIntDoubleVector(compVector.getDim, compVector.getPartitions.map { comp =>
                sumVector.sub(comp).mul(grad).asInstanceOf[IntDoubleVector]
              })
            }

            MFactory.rbCompIntDoubleMatrix(gradRows)
          case RowType.T_FLOAT_DENSE =>
            val inputData = inputLayer.calOutput().asInstanceOf[RBCompIntFloatMatrix]

            val gradRows = inputData.getRows.zipWithIndex.map { case (compVector, idx) =>
              val sumVector = VectorUtils.emptyLike(compVector.getPartitions.head.asInstanceOf[Vector])
              compVector.getPartitions.foreach(comp => sumVector.iadd(comp))

              val grad = gradTemp.getRow(idx)

              VFactory.compIntFloatVector(compVector.getDim, compVector.getPartitions.map { comp =>
                sumVector.sub(comp).mul(grad).asInstanceOf[IntFloatVector]
              })
            }

            MFactory.rbCompIntFloatMatrix(gradRows)
        }

        status = STATUS.Gradient
      case _ =>
    }
    gradOutput
  }

}
