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

import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.math2.matrix.{BlasDoubleMatrix, BlasFloatMatrix, Matrix}
import com.tencent.angel.ml.math2.storage.{IntDoubleDenseVectorStorage, IntFloatDenseVectorStorage}
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.utils.MLException
import org.apache.commons.logging.LogFactory


class DotPooling(name: String, outputDim: Int, inputLayers: Array[Layer])(implicit graph: Graph)
  extends JoinLayer(name, outputDim, inputLayers) {
  private val LOG = LogFactory.getLog(classOf[DotPooling])

  @transient private var opTemp: Matrix = _

  override protected def doForward(inputs: Map[String, Matrix]): Matrix = {
    val mats = inputs.values.toList

    if (inputLayers.length == 2) {
      opTemp = Ufuncs.mul(mats.head, mats(1))
      opTemp.sum(1).getStorage match {
        case s: IntDoubleDenseVectorStorage =>
          MFactory.denseDoubleMatrix(opTemp.getNumRows, 1, s.getValues)
        case s: IntFloatDenseVectorStorage =>
          MFactory.denseFloatMatrix(opTemp.getNumRows, 1, s.getValues)
      }
    } else if (inputLayers.length > 2) {
      val opTemp = Ufuncs.mul(mats.head, mats(1))
      mats.tail.tail.foreach(mat => opTemp.imul(mat))

      opTemp.sum(1).getStorage match {
        case s: IntDoubleDenseVectorStorage =>
          MFactory.denseDoubleMatrix(opTemp.getNumRows, 1, s.getValues)
        case s: IntFloatDenseVectorStorage =>
          MFactory.denseFloatMatrix(opTemp.getNumRows, 1, s.getValues)
      }
    } else {
      throw MLException("At least two layers are required as input!")
    }
  }

  override protected def doBackward(inputs: Map[String, Matrix], gradInput: Matrix): Map[String, Matrix] = {

    if (inputLayers.length == 2) {
      inputs.map { case (layerName, _: Matrix) =>
        val otherOutput = inputs.collect{
          case (otherName, otherMat: Matrix) if otherName != layerName => otherMat
        }.head

        val gradOutput = gradInput match {
          case grad: BlasDoubleMatrix =>
            val gradVector = VFactory.denseDoubleVector(grad.getData)
            Ufuncs.mul(otherOutput, gradVector, true)
          case grad: BlasFloatMatrix =>
            val gradVector = VFactory.denseFloatVector(grad.getData)
            Ufuncs.mul(otherOutput, gradVector, true)
        }

        layerName -> gradOutput
      }
    } else if (inputLayers.length > 2) {
      inputs.map { case (layerName: String, mat: Matrix) =>
        val otherOutput = opTemp.div(mat)
        val gradOutput = gradInput match {
          case grad: BlasDoubleMatrix =>
            val gradVector = VFactory.denseDoubleVector(grad.getData)
            Ufuncs.mul(otherOutput, gradVector, true)
          case grad: BlasFloatMatrix =>
            val gradVector = VFactory.denseFloatVector(grad.getData)
            Ufuncs.mul(otherOutput, gradVector, true)
        }

        layerName -> gradOutput
      }
    } else {
      throw MLException("At least two layers are required as input!")
    }
  }

  override def toString: String = {
    s"DotPooling name=$name outputDim=$outputDim"
  }

}
