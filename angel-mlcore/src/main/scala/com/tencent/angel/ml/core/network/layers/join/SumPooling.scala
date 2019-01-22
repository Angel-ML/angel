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
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.core.network.layers._
import org.apache.commons.logging.LogFactory


class SumPooling(name: String, outputDim: Int, inputLayers: Array[Layer])(implicit graph: Graph)
  extends JoinLayer(name, outputDim, inputLayers) {
  private val LOG = LogFactory.getLog(classOf[SumPooling])

  override protected def doForward(inputs: Map[String, Matrix]): Matrix = {
    var output: Matrix = null

    inputs.foreach{ case (_, mat: Matrix) =>
        if (output == null) {
          output = mat.copy()
        } else {
          output.iadd(mat)
        }
    }
    output
  }

  override protected def doBackward(inputs: Map[String, Matrix], gradInput: Matrix): Map[String, Matrix] = {
    inputs.map{ case (layerName: String, _) => layerName -> gradInput }
  }

  override def toString: String = {
    s"SumPooling name=$name outputDim=$outputDim"
  }
}
