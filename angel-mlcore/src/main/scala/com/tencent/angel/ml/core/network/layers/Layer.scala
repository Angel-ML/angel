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

import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.optimizer.Optimizer
import com.tencent.angel.ml.core.utils.{LayerKeys, MLException}
import com.tencent.angel.ml.core.variable.VariableManager
import com.tencent.angel.ml.math2.matrix.{BlasDoubleMatrix, BlasFloatMatrix, BlasMatrix, Matrix, RBCompIntDoubleMatrix, RBCompIntFloatMatrix}
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import com.tencent.angel.ml.math2.utils.MatrixUtils
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.native.Serialization
import org.json4s.{Formats, ShortTypeHints}

import scala.collection.mutable

trait Trainable {
  def optimizer: Optimizer
}

abstract class Layer(val name: String, val outputDim: Int)(implicit val graph: Graph) extends Serializable {
  private val inputs = new mutable.HashMap[String, Layer]()
  private val consumer = new mutable.HashMap[String, Layer]()

  protected val variableManager: VariableManager = graph.provider.variableManager

  protected val forwardKey: String = s"$name/forward"
  protected val backwardKey: String = s"$name/backward"

  protected implicit val formats: Formats = Serialization.formats(ShortTypeHints(List()))

  def addInput(layer: Layer): Unit = {
    inputs.put(layer.name, layer)
  }

  def removeInput(layer: Layer): Unit = {
    if (inputs.contains(layer.name)) {
      inputs.remove(layer.name)
    }
  }

  def removeInput(layerName: String): Unit = {
    if (inputs.contains(layerName)) {
      inputs.remove(layerName)
    }
  }

  def isInput(layerName: String): Boolean = inputs.contains(layerName)

  def isInput(layer: Layer): Boolean = inputs.contains(layer.name)

  def getInput(name: String): Layer = {
    inputs.getOrElse(name, null.asInstanceOf[Layer])
  }

  def getAllInputs: List[Layer] = inputs.values.toList

  def getAllInputNames: List[String] = inputs.keys.toList

  def addConsumer(layer: Layer): Unit = {
    consumer.put(layer.name, layer)
  }

  def removeConsumer(layer: Layer): Unit = {
    if (consumer.contains(layer.name)) {
      consumer.remove(layer.name)
    }
  }

  def removeConsumer(layerName: String): Unit = {
    if (consumer.contains(layerName)) {
      consumer.remove(layerName)
    }
  }

  def isConsumer(layerName: String): Boolean = consumer.contains(layerName)

  def isConsumer(layer: Layer): Boolean = consumer.contains(layer.name)

  def getConsumer(name: String): Layer = {
    consumer.getOrElse(name, null.asInstanceOf[Layer])
  }

  def getAllConsumers: List[Layer] = consumer.values.toList

  def getAllConsumerNames: List[String] = consumer.keys.toList

  def forward(): Matrix

  def backward(layer: Layer): Matrix

  protected def gatherGradInput(): Matrix = {
    var gradCollection: Matrix = null
    getAllConsumers.foreach { csLayer =>
      if (gradCollection == null) {
        gradCollection = csLayer.backward(this).copy()
      } else {
        val temp_ = csLayer.backward(this)
        val temp = gradCollection match {
          case x: BlasMatrix =>
            temp_ match {
              case y: BlasMatrix =>
                y
              case y: RBCompIntDoubleMatrix =>
                MatrixUtils.rbCompDense2Blas(y.asInstanceOf[RBCompIntDoubleMatrix])
              case y: RBCompIntFloatMatrix =>
                MatrixUtils.rbCompDense2Blas(y.asInstanceOf[RBCompIntFloatMatrix])
            }
          case x: RBCompIntDoubleMatrix =>
            temp_ match {
              case y: BlasDoubleMatrix =>
                MatrixUtils.blas2RBCompDense(y.asInstanceOf[BlasDoubleMatrix], gradCollection.asInstanceOf[RBCompIntDoubleMatrix].getSubDim)
              case y: RBCompIntDoubleMatrix =>
                y
            }
          case x: RBCompIntFloatMatrix =>
            temp_ match {
              case y: BlasFloatMatrix =>
                MatrixUtils.blas2RBCompDense(y.asInstanceOf[BlasDoubleMatrix], gradCollection.asInstanceOf[RBCompIntDoubleMatrix].getSubDim)
              case y: RBCompIntFloatMatrix =>
                y
            }
        }
        gradCollection.iadd(temp)
      }
    }

    gradCollection
  }

  def toJson: JField = {
    val layerJson = (LayerKeys.typeKey -> s"${this.getClass.getSimpleName}") ~
      (LayerKeys.outputDimKey -> outputDim)

    JField(name, layerJson)
  }
}
