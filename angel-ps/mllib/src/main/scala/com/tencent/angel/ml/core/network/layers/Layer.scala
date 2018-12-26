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


package com.tencent.angel.ml.core.network.layers

import java.util.concurrent.Future

import com.google.gson.Gson
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.optimizer.loss.LossFunc
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult
import com.tencent.angel.model.{ModelLoadContext, ModelSaveContext}

import scala.collection.mutable.ListBuffer

object STATUS extends Enumeration {
  type STATUS = Value
  val Null, Forward, Backward, Gradient, Update = Value
}

trait Trainable {
  def optimizer: Optimizer

  def pullParams(epoch: Int): Unit

  def pushGradient(): Unit

  def update(epoch: Int, batchSize: Int): Future[VoidResult]

  def init(taskId: Int)

  def loadParams(loadContext: ModelLoadContext): Unit

  def saveParams(saveContext: ModelSaveContext): Unit
}

trait LossLayer {
  def predict(): Matrix

  def calLoss(): Double

  def getLossFunc(): LossFunc
}

abstract class Layer(val name: String, val outputDim: Int)(implicit val graph: AngelGraph)
  extends Serializable {
  var status: STATUS.Value = STATUS.Null
  val input = new ListBuffer[Layer]()
  val consumer = new ListBuffer[Layer]()

  def addInput(layer: Layer): Unit = {
    input.append(layer)
  }

  def addConsumer(layer: Layer): Unit = {
    consumer.append(layer)
  }

  def calOutput(): Matrix

  def gatherGrad(): Matrix = {
    if (consumer.length == 1) {
      consumer.head match {
        case layer: LinearLayer =>
          layer.calGradOutput()
        case layer: JoinLayer =>
          var tempGrad: Matrix = null
          layer.inputLayers.zipWithIndex.foreach {
            case (l, idx) if l.name == this.name =>
              tempGrad = layer.calGradOutput(idx)
            case _ =>
          }
          tempGrad
      }
    } else {
      var gradCollection: Matrix = null

      consumer.foreach {
        case layer: LinearLayer =>
          if (null == gradCollection) {
            gradCollection = layer.calGradOutput().copy()
          } else {
            gradCollection.iadd(layer.calGradOutput())
          }
        case layer: JoinLayer =>
          var tempGrad: Matrix = null
          layer.inputLayers.zipWithIndex.foreach {
            case (l, idx) if l.name == this.name =>
              tempGrad = layer.calGradOutput(idx)
            case _ =>
          }

          if (null == gradCollection) {
            gradCollection = tempGrad.copy()
          } else {
            gradCollection.iadd(tempGrad)
          }
      }

      gradCollection
    }
  }

  def toMeta(): LayerMeta = ???
}

abstract class LayerMeta(name: String, outputDim: Int) extends Serializable {

  def toJson(): String = {
    new Gson().toJson(this)
  }

}

abstract class InputLayer(name: String, outputDim: Int)(implicit graph: AngelGraph)
  extends Layer(name, outputDim)(graph) {
  graph.addInput(this)

  def calBackward(): Matrix
}

abstract class InputLayerMeta(name: String, outputDim: Int) extends LayerMeta(name, outputDim) {

}

abstract class JoinLayer(name: String, outputDim: Int, val inputLayers: Array[Layer])(implicit graph: AngelGraph)
  extends Layer(name, outputDim)(graph) {
  inputLayers.foreach { layer =>
    layer.addConsumer(this)
    this.addInput(layer)
  }

  def calGradOutput(idx: Int): Matrix
}

abstract class JoinLayerMeta(name: String, outputDim: Int, inputLayers: Array[String]) extends LayerMeta(name, outputDim) {

}


abstract class LinearLayer(name: String, outputDim: Int, val inputLayer: Layer)(implicit graph: AngelGraph)
  extends Layer(name, outputDim)(graph) {
  inputLayer.addConsumer(this)
  this.addInput(inputLayer)

  def calGradOutput(): Matrix
}

abstract class LinearLayerMeta(name: String, outputDim: Int, inputLayer: String) extends LayerMeta(name, outputDim) {

}
