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

import com.tencent.angel.ml.core.PredictResult
import com.tencent.angel.ml.core.data.DataBlock
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.optimizer.loss.LossFunc
import com.tencent.angel.ml.core.utils.{Callback, LayerKeys}
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.ShortTypeHints
import org.json4s.native.Serialization

import scala.collection.mutable.ListBuffer



object STATUS extends Enumeration {
  type STATUS = Value
  val Null, Forward, Backward, Gradient, Update = Value
}

trait Trainable {
  def init(taskId: Int)

  def optimizer: Optimizer

  def pullParams(epoch: Int): Unit

  def pushGradient(): Unit

  def update[T](epoch: Int, batchSize: Int)(callback: Callback[T]): Callback[T]

  def load(): Unit

  def save(): Unit
}

trait LossLayer {
  def predict(): List[PredictResult]

  def calLoss(): Double

  def getLossFunc: LossFunc
}

abstract class Layer(val name: String, val outputDim: Int)(implicit val graph: Graph) extends Serializable {
  var status: STATUS.Value = STATUS.Null
  val input = new ListBuffer[Layer]()
  val consumer = new ListBuffer[Layer]()

  protected implicit val formats = Serialization.formats(ShortTypeHints(List()))

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

  def toJson(): JField = {
    val layerJson = (LayerKeys.typeKey -> s"${this.getClass.getSimpleName}") ~
      (LayerKeys.outputDimKey -> outputDim)
    JField(name, layerJson)
  }

}


abstract class InputLayer(name: String, outputDim: Int)(implicit graph: Graph)
  extends Layer(name, outputDim) {
  graph.addInput(this)

  def calBackward(): Matrix
}


abstract class JoinLayer(name: String, outputDim: Int, val inputLayers: Array[Layer])(implicit graph: Graph)
  extends Layer(name, outputDim) {
  inputLayers.foreach { layer =>
    layer.addConsumer(this)
    this.addInput(layer)
  }

  def calGradOutput(idx: Int): Matrix

  override def toJson(): JField = {
    val layerJson = (LayerKeys.typeKey -> s"${this.getClass.getSimpleName}") ~
      (LayerKeys.outputDimKey -> outputDim) ~
      (LayerKeys.inputLayersKey -> JArray(inputLayers.toList.map(layer => JString(layer.name))))

    JField(name, layerJson)
  }
}


abstract class LinearLayer(name: String, outputDim: Int, val inputLayer: Layer)(implicit graph: Graph)
  extends Layer(name, outputDim) {
  inputLayer.addConsumer(this)
  this.addInput(inputLayer)

  def calGradOutput(): Matrix

  override def toJson(): JField = {
    val layerJson = (LayerKeys.typeKey -> s"${this.getClass.getSimpleName}") ~
      (LayerKeys.outputDimKey -> outputDim) ~
      (LayerKeys.inputLayerKey, JString(inputLayer.name))

    JField(name, layerJson)
  }
}


