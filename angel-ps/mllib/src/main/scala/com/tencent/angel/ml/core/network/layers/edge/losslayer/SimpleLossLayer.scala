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


package com.tencent.angel.ml.core.network.layers.edge.losslayer

import com.tencent.angel.ml.math2.matrix._
import com.tencent.angel.ml.core.network.layers._
import com.tencent.angel.ml.core.optimizer.loss.LossFunc
import org.apache.commons.logging.LogFactory


class SimpleLossLayer(name: String, inputLayer: Layer, lossFunc: LossFunc)(
  implicit graph: AngelGraph) extends LinearLayer(name, 1, inputLayer)(graph) with LossLayer {
  val LOG = LogFactory.getLog(classOf[SimpleLossLayer])
  graph.setOutput(this)

  @transient var output: Matrix = _
  @transient var gradOutput: Matrix = _
  @transient var loss: Double = Double.NaN

  override def calGradOutput(): Matrix = {
    val start = System.currentTimeMillis()
    status match {
      case STATUS.Forward =>
        //        println(s"the status in SimpleLossLayer($name)-calGradOutput is ${status.toString}")
        gradOutput = lossFunc.calGrad(output, graph)
        status = STATUS.Backward
      case _ =>
    }
    val end = System.currentTimeMillis()
    //    println(s"SimpleLossLayer($name) calGradOutput = ${end - start} ms")
    gradOutput
  }

  override def calLoss(): Double = {
    status match {
      case STATUS.Null =>
        calOutput()
        loss = lossFunc.calLoss(output, graph)
      case STATUS.Forward =>
        loss = lossFunc.calLoss(output, graph)
      case _ =>
    }

    loss
  }

  override def predict(): Matrix = {
    status match {
      case STATUS.Null =>
        calOutput()
      case _ =>
    }

    lossFunc.predict(output)
  }

  override def calOutput(): Matrix = {
    val start = System.currentTimeMillis()
    status match {
      case STATUS.Null =>
        //        println(s"the status in SimpleLossLayer($name)-calOutput is ${status.toString}")
        output = inputLayer.calOutput()
        status = STATUS.Forward
      case _ =>
    }
    val end = System.currentTimeMillis()
    //    println(s"SimpleLossLayer($name) calOutput = ${end - start} ms")

    output
  }

  override def toString: String = {
    s"SimpleLossLayer lossFunc=$lossFunc"
  }
}
