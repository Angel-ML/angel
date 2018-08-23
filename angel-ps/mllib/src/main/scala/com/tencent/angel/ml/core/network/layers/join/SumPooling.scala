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

import com.tencent.angel.ml.math2.matrix.{BlasDoubleMatrix, BlasFloatMatrix, Matrix}
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.math2.utils.{MatrixUtils, VectorUtils}
import org.apache.commons.logging.LogFactory


class SumPooling(name: String, outputDim: Int, inputLayers: Array[Layer])(implicit graph: AngelGraph)
  extends JoinLayer(name, outputDim, inputLayers)(graph) {
  val LOG = LogFactory.getLog(classOf[SumPooling])

  @transient var output: Matrix = _
  @transient var gradOutput: Array[Matrix] = _

  override def calOutput(): Matrix = {
    val start = System.currentTimeMillis()
    status match {
      case STATUS.Null =>
        //        println(s"the status in SumPooling($name)-calOutput is ${status.toString}")
        inputLayers.zipWithIndex.foreach {
          case (layer, idx) if idx == 0 =>
            output = layer.calOutput().copy()
          case (layer, idx) if idx > 0 => output.iadd(layer.calOutput())
        }
        status = STATUS.Forward
      case _ =>
    }

    val end = System.currentTimeMillis()
    //    println(s"SumPooling($name) calOutput = ${end - start} ms")
    output
  }

  override def calGradOutput(idx: Int): Matrix = {
    val start = System.currentTimeMillis()
    status match {
      case STATUS.Forward =>
        val gradTemp = gatherGrad()
        //        println(s"the status in SumPooling($name)-calGradOutput is ${status.toString}")
        gradOutput = inputLayers.indices.toArray.map { _ => gradTemp }
        status = STATUS.Backward
      case _ =>
    }
    val end = System.currentTimeMillis()
    //    println(s"SumPooling($name) calGradOutput = ${end - start} ms")
    gradOutput(idx)
  }

  override def toString: String = {
    s"SumPooling name=$name outputDim=$outputDim"
  }
}
