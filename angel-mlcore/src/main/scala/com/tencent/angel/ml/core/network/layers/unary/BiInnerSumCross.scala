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
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.vector._
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import org.apache.commons.logging.{Log, LogFactory}


class BiInnerSumCross(name: String, inputLayer: Layer)(implicit graph: Graph)
  extends LinearLayer(name, 1, inputLayer) {
  val LOG: Log = LogFactory.getLog(classOf[BiInnerSumCross])

  override protected def doForward(input: Matrix): Matrix = {
    val batchSize = graph.getBatchSize
    input match {
      case mat: RBCompIntDoubleMatrix =>
        val data: Array[Double] = new Array[Double](batchSize)
        val sumVector = VFactory.denseDoubleVector(mat.getSubDim)
        (0 until batchSize).foreach { row =>
          val partitions = mat.getRow(row).getPartitions
          partitions.foreach { vectorOuter =>
            data(row) -= vectorOuter.dot(vectorOuter)
            sumVector.iadd(vectorOuter)
          }
          data(row) += sumVector.dot(sumVector)
          data(row) /= 2
          sumVector.clear()

          // data(row) = getInnerSum(mat.getRow(row).getPartitions)
        }
        MFactory.denseDoubleMatrix(batchSize, 1, data)
      case mat: RBCompIntFloatMatrix =>
        val data: Array[Float] = new Array[Float](batchSize)
        val sumVector = VFactory.denseFloatVector(mat.getSubDim)

        (0 until batchSize).foreach { row =>
          val partitions = mat.getRow(row).getPartitions

          partitions.foreach { vectorOuter =>
            data(row) -= vectorOuter.dot(vectorOuter).toFloat
            sumVector.iadd(vectorOuter)
          }

          data(row) += sumVector.dot(sumVector).toFloat
          data(row) /= 2
          sumVector.clear()
          // data(row) = getInnerSum(mat.getRow(row).getPartitions)
        }
        MFactory.denseFloatMatrix(batchSize, 1, data)
      case _ => throw MLException("ERROR! Only Comp Matrix is supported!")
    }
  }

  override protected def doBackward(input: Matrix, gradInput: Matrix): Matrix = {
    conf.valueType() match {
      case "double" =>
        val inputData = input.asInstanceOf[RBCompIntDoubleMatrix]
        val sumVector = VFactory.denseDoubleVector(inputData.getSubDim)
        val gradRows = inputData.getRows.zipWithIndex.map { case (compVector, idx) =>
          compVector.getPartitions.foreach(comp => sumVector.iadd(comp))

          val grad = gradInput.asInstanceOf[BlasDoubleMatrix].getData()(idx)

          val gradRow = VFactory.compIntDoubleVector(compVector.getDim, compVector.getPartitions.map { comp =>
            sumVector.sub(comp).imul(grad).asInstanceOf[IntDoubleVector]
          })

          sumVector.clear()
          gradRow
        }

        MFactory.rbCompIntDoubleMatrix(gradRows)
      case "float" =>
        val inputData = input.asInstanceOf[RBCompIntFloatMatrix]
        val sumVector = VFactory.denseFloatVector(inputData.getSubDim)
        val gradRows = inputData.getRows.zipWithIndex.map { case (compVector, idx) =>
          compVector.getPartitions.foreach(comp => sumVector.iadd(comp))

          val grad = gradInput.asInstanceOf[BlasFloatMatrix].getData()(idx)

          val gradRow = VFactory.compIntFloatVector(compVector.getDim, compVector.getPartitions.map { comp =>
            sumVector.sub(comp).imul(grad).asInstanceOf[IntFloatVector]
          })

          sumVector.clear()
          gradRow
        }

        MFactory.rbCompIntFloatMatrix(gradRows)
      case _ => throw MLException("Only Double and Float are support!")
    }
  }

  def getInnerSum(vectors: Array[IntDoubleVector]): Double = {
    val values = vectors.map(f => f.getStorage.getValues)
    var f = 0
    var i = 0
    val size = vectors(0).size()
    var result = 0.0
    while (f < size) {
      var sum = 0.0
      var square_sum = 0.0
      i = 0
      while (i < vectors.length) {
        val value = values(i)(f)
        sum += value
        square_sum += value * value
        i += 1
      }
      result += sum * sum - square_sum
      f += 1
    }
    result / 2
  }

  def getInnerSum(vectors: Array[IntFloatVector]): Float = {

    val values = vectors.map(f => f.getStorage.getValues)
    var f = 0
    var i = 0
    val size = vectors(0).size()
    var result = 0.0F
    while (f < size) {
      var sum = 0.0F
      var square_sum = 0.0F
      i = 0
      while (i < vectors.length) {
        val value = values(i)(f)
        sum += value
        square_sum += value * value
        i += 1
      }
      result += sum * sum - square_sum
      f += 1
    }
    result / 2
  }
}
