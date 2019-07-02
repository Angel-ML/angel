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

import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.utils.MLException
import com.tencent.angel.ml.servingmath2.MFactory
import com.tencent.angel.ml.servingmath2.matrix._
import com.tencent.angel.ml.servingmath2.utils.MatrixUtils
import com.tencent.angel.ml.servingmath2.vector.{IntDoubleVector, IntFloatVector}
import org.apache.commons.logging.LogFactory


class ConcatLayer(name: String, outputDim: Int, inputLayers: Array[Layer])(implicit graph: Graph)
  extends JoinLayer(name, outputDim, inputLayers) {
  private val LOG = LogFactory.getLog(classOf[ConcatLayer])
  assert(inputLayers.map(_.outputDim).sum == outputDim)

  override protected def doForward(inputs: Map[String, Matrix]): Matrix = {
    var start = 0
    val batchSize = graph.placeHolder.getBatchSize

    conf.valueType() match {
      case "double" =>
        val outTemp = MFactory.denseDoubleMatrix(batchSize, outputDim)
        val outData = outTemp.getData

        (0 until batchSize).foreach { row =>
          inputLayers.foreach { layer =>
            inputs(layer.name) match {
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
              case _ => throw MLException("ERROR! the matrix type is not supported!")
            }

          }
        }

        outTemp
      case "float" =>
        val outTemp = MFactory.denseFloatMatrix(batchSize, outputDim)
        val outData = outTemp.getData
        (0 until batchSize).foreach { row =>
          inputLayers.foreach { layer =>
            inputs(layer.name) match {
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
              case _ => throw MLException("ERROR! the matrix type is not supported!")
            }
          }
        }

        outTemp
      case _ => throw MLException("Only Double and Float are support!")
    }

  }

  override protected def doBackward(inputs: Map[String, Matrix], gradInput: Matrix): Map[String, Matrix] = {
    var start = 0
    val batchSize = graph.placeHolder.getBatchSize
    val gradOutput = inputs.map { case (layerName, mat) => layerName -> MatrixUtils.emptyLike(mat) }

    conf.valueType() match {
      case "double" =>
        val grad = gradInput.asInstanceOf[BlasDoubleMatrix].getData
        (0 until batchSize).foreach { row =>
          inputLayers.foreach { layer =>
            gradOutput(layer.name) match {
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
        }

      case "float" =>
        val grad = gradInput.asInstanceOf[BlasFloatMatrix].getData
        (0 until batchSize).foreach { row =>
          inputLayers.foreach { layer =>
            gradOutput(layer.name) match {
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
        }
      case _ => throw MLException("Only Double and Float are support!")
    }

    gradOutput

  }

  override def toString: String = {
    s"ConcatLayer name=$name outputDim=$outputDim"
  }

}