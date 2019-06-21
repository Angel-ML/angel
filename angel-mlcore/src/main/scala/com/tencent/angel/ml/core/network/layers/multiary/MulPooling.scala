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
import com.tencent.angel.ml.servingmath2.matrix.Matrix
import com.tencent.angel.ml.servingmath2.ufuncs.Ufuncs
import org.apache.commons.logging.LogFactory


class MulPooling(name: String, outputDim: Int, inputLayers: Array[Layer])(implicit graph: Graph)
  extends JoinLayer(name, outputDim, inputLayers) {
  val LOG = LogFactory.getLog(classOf[MulPooling])

  @transient private var opTemp: Matrix = _

  override protected def doForward(inputs: Map[String, Matrix]): Matrix = {
    val mats = inputs.values.toList
    if (inputLayers.length == 2) {
      opTemp = Ufuncs.mul(mats.head, mats(1))
    } else if (inputLayers.length > 2) {
      opTemp = Ufuncs.mul(mats.head, mats(1))
      mats.tail.tail.foreach(mat => opTemp.imul(mat))
    } else {
      throw MLException("At least two layers are required as input!")
    }

    opTemp
  }

  override protected def doBackward(inputs: Map[String, Matrix], gradInput: Matrix): Map[String, Matrix] = {
    if (inputLayers.length == 2) {
      inputs.map { case (layerName, _: Matrix) =>
        inputs.collectFirst {
          case (otherName, otherMat: Matrix) if otherName != layerName =>
            layerName -> gradInput.mul(otherMat)
        }.get
      }
    }
    else if (inputLayers.length > 2) {
      inputs.map { case (layerName: String, mat: Matrix) =>
        layerName -> gradInput.mul(opTemp.div(mat))
      }
    } else {
      throw MLException("At least two layers are required as input!")
    }
  }


  override def toString: String = {
    s"MulPooling name=$name outputDim=$outputDim"
  }


}
