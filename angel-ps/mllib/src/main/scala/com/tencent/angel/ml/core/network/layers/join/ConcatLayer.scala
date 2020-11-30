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

import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.math2.MFactory
import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.math2.utils.MatrixUtils
import com.tencent.angel.ml.math2.vector.{IntDoubleVector, IntFloatVector}
import org.apache.commons.logging.LogFactory


class ConcatLayer(name: String, outputDim: Int, inputLayers: Array[Layer])(implicit graph: AngelGraph)
  extends JoinLayer(name, outputDim, inputLayers)(graph) {
  val LOG = LogFactory.getLog(classOf[ConcatLayer])

  val valueType: String = SharedConf.valueType()
  assert(inputLayers.map(_.outputDim).sum == outputDim)

  @transient var output: Matrix = _
  @transient var gradOutput: Array[Matrix] = _

  override def calOutput(): Matrix = {

    status match {
      case STATUS.Null =>
        var start = 0
        val batchSize = graph.placeHolder.getBatchSize
        val outputMat = inputLayers.map { layer => layer.calOutput() }

        output = valueType match {
          case "double" =>
            val outTemp = MFactory.denseDoubleMatrix(batchSize, outputDim)
            val outData = outTemp.getData

            (0 until batchSize).foreach { row =>
              outputMat.foreach {
                case mat: RBCompIntDoubleMatrix =>
                  mat.getRow(row).getPartitions.foreach { comp =>
                    assert(comp.isDense)
                    val compValues = comp.getStorage.getValues
                    Array.copy(compValues, 0, outData, start, compValues.length)
                    start += compValues.length
                  }
                case mat: BlasDoubleMatrix if mat.getNumCols > 1 =>
                  val values = mat.getRow(row).asInstanceOf[IntDoubleVector].getStorage.getValues
                  Array.copy(values, 0, outData, start, values.length)
                  start += values.length
                case mat: BlasDoubleMatrix if mat.getNumCols == 1 =>
                  outData(start) = mat.get(row, 0)
                  start += 1
                case _ => throw new AngelException("ERROR! the matrix type is not supported!")
              }

            }

            outTemp
          case "float" =>
            val outTemp = MFactory.denseFloatMatrix(batchSize, outputDim)
            val outData = outTemp.getData
            (0 until batchSize).foreach { row =>
              outputMat.foreach {
                case mat: RBCompIntFloatMatrix =>
                  mat.getRow(row).getPartitions.foreach { comp =>
                    assert(comp.isDense)
                    val compValues = comp.getStorage.getValues
                    Array.copy(compValues, 0, outData, start, compValues.length)
                    start += compValues.length
                  }
                case mat: BlasFloatMatrix if mat.getNumCols > 1 =>
                  val values = mat.getRow(row).asInstanceOf[IntFloatVector].getStorage.getValues
                  Array.copy(values, 0, outData, start, values.length)
                  start += values.length
                case mat: BlasFloatMatrix if mat.getNumCols == 1 =>
                  outData(start) = mat.get(row, 0)
                  start += 1
                case _ => throw new AngelException("ERROR! the matrix type is not supported!")
              }
            }

            outTemp
          case _ => throw new AngelException("Only Double and Float are support!")
        }
        status = STATUS.Forward
      case _ =>
    }
    output
  }

  override def calGradOutput(idx: Int): Matrix = {
    status match {
      case STATUS.Forward =>
        val gradTemp = gatherGrad()

        var start = 0
        val batchSize = graph.placeHolder.getBatchSize
        gradOutput = inputLayers.map { layer => MatrixUtils.emptyLike(layer.calOutput()) }

        valueType match {
          case "double" =>
            val grad = gradTemp.asInstanceOf[BlasDoubleMatrix].getData
            (0 until batchSize).foreach { row =>
              gradOutput.foreach {
                case mat: RBCompIntDoubleMatrix =>
                  mat.getRow(row).getPartitions.foreach { part =>
                    assert(part.isDense)

                    val values = part.getStorage.getValues
                    Array.copy(grad, start, values, 0, values.length)
                    start += values.length
                  }
                case mat: BlasDoubleMatrix if mat.getNumCols > 1 =>
                  val cols = mat.getNumCols
                  Array.copy(grad, start, mat.getData, row * cols, cols)
                  start += cols
                case mat: BlasDoubleMatrix if mat.getNumCols == 1 =>
                  mat.set(row, 0, grad(start))
                  start += 1
              }
            }

          case "float" =>
            val grad = gradTemp.asInstanceOf[BlasFloatMatrix].getData
            (0 until batchSize).foreach { row =>
              gradOutput.foreach {
                case mat: RBCompIntFloatMatrix =>
                  mat.getRow(row).getPartitions.foreach { part =>
                    assert(part.isDense)

                    val values = part.getStorage.getValues
                    Array.copy(grad, start, values, 0, values.length)
                    start += values.length
                  }
                case mat: BlasFloatMatrix if mat.getNumCols > 1 =>
                  val cols = mat.getNumCols
                  Array.copy(grad, start, mat.getData, row * cols, cols)
                  start += cols
                case mat: BlasFloatMatrix if mat.getNumCols == 1 =>
                  mat.set(row, 0, grad(start))
                  start += 1
              }
            }
          case _ => throw new AngelException("Only Double and Float are support!")
        }

        status = STATUS.Backward
      case _ =>
    }

    gradOutput(idx)
  }

  override def toString: String = {
    s"ConcatLayer name=$name outputDim=$outputDim"
  }
}