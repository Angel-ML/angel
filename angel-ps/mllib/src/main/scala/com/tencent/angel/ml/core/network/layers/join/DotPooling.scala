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
import com.tencent.angel.ml.math2.matrix.{BlasDoubleMatrix, BlasFloatMatrix, Matrix}
import com.tencent.angel.ml.math2.storage.{IntDoubleDenseVectorStorage, IntFloatDenseVectorStorage}
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import com.tencent.angel.ml.core.network.layers._
import org.apache.commons.logging.LogFactory


class DotPooling(name: String, outputDim: Int, inputLayers: Array[Layer])(implicit graph: AngelGraph)
  extends JoinLayer(name, outputDim, inputLayers)(graph) {
  val LOG = LogFactory.getLog(classOf[DotPooling])

  @transient var output: Matrix = _
  @transient var opTemp: Matrix = _
  @transient var gradOutput: Array[Matrix] = _

  override def calOutput(): Matrix = {
    status match {
      case STATUS.Null | STATUS.Update =>
        if (inputLayers.length == 2) {
          opTemp = Ufuncs.mul(inputLayers(0).calOutput(), inputLayers(1).calOutput())
          opTemp.sum(1).getStorage match {
            case s: IntDoubleDenseVectorStorage =>
              output = MFactory.denseDoubleMatrix(opTemp.getNumRows, 1, s.getValues)
            case s: IntFloatDenseVectorStorage =>
              output = MFactory.denseFloatMatrix(opTemp.getNumRows, 1, s.getValues)
          }
        } else if (inputLayers.length > 2) {
          opTemp = Ufuncs.mul(inputLayers(0).calOutput(), inputLayers(1).calOutput())
          inputLayers.tail.tail.foreach(layer => opTemp = opTemp.mul(layer.calOutput()))

          opTemp.sum(1).getStorage match {
            case s: IntDoubleDenseVectorStorage =>
              output = MFactory.denseDoubleMatrix(opTemp.getNumRows, 1, s.getValues)
            case s: IntFloatDenseVectorStorage =>
              output = MFactory.denseFloatMatrix(opTemp.getNumRows, 1, s.getValues)
          }
        } else {
          throw new AngelException("At least two layers are required as input!")
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

        if (inputLayers.length == 2) {
          gradOutput = inputLayers.indices.toArray.map { i =>
            val otherOutput = inputLayers((i + 1) % inputLayers.length).calOutput()
            gradTemp match {
              case grad: BlasDoubleMatrix =>
                val gradVector = VFactory.denseDoubleVector(grad.getData)
                Ufuncs.mul(otherOutput, gradVector, true)
              case grad: BlasFloatMatrix =>
                val gradVector = VFactory.denseFloatVector(grad.getData)
                Ufuncs.mul(otherOutput, gradVector, true)
            }
          }
        } else if (inputLayers.length > 2) {
          gradOutput = inputLayers.map { layer =>
            val otherOutput = opTemp.div(layer.calOutput())
            gradTemp match {
              case grad: BlasDoubleMatrix =>
                val gradVector = VFactory.denseDoubleVector(grad.getData)
                Ufuncs.mul(otherOutput, gradVector, true)
              case grad: BlasFloatMatrix =>
                val gradVector = VFactory.denseFloatVector(grad.getData)
                Ufuncs.mul(otherOutput, gradVector, true)
            }
          }
        } else {
          throw new AngelException("At least two layers are required as input!")
        }

        status = STATUS.Backward
      case _ =>
    }
    gradOutput(idx)
  }

  override def toString: String = {
    s"DotPooling name=$name outputDim=$outputDim"
  }
}
