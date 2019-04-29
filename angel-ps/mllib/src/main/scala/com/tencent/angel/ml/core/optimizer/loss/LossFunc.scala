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


package com.tencent.angel.ml.core.optimizer.loss

import com.tencent.angel.ml.core.conf.SharedConf
import com.tencent.angel.ml.math2.{MFactory, VFactory}
import com.tencent.angel.ml.math2.vector.{IntDoubleVector, IntFloatVector, Vector}
import com.tencent.angel.ml.math2.matrix.{BlasDoubleMatrix, BlasFloatMatrix, BlasMatrix, Matrix}
import com.tencent.angel.ml.math2.ufuncs.{LossFuncs, TransFuncs, Ufuncs}
import com.tencent.angel.ml.core.network.layers.AngelGraph
import com.tencent.angel.ml.core.utils.paramsutils.ParamKeys
import org.json4s.JsonAST.{JField, JObject, JString}
import org.json4s.JsonDSL._

trait LossFunc extends Serializable {
  def calLoss(modelOut: Matrix, graph: AngelGraph): Double

  def loss(pred: Double, label: Double): Double

  def calGrad(modelOut: Matrix, graph: AngelGraph): Matrix

  def predict(modelOut: Matrix, graph: AngelGraph): Matrix

  def toJson: JObject = {
    JObject(JField(ParamKeys.typeName, JString(s"${this.getClass.getSimpleName}")))
  }
}

class L2Loss extends LossFunc {
  override def calLoss(modelOut: Matrix, graph: AngelGraph): Double = {
    modelOut match {
      case mat: BlasMatrix =>
        0.5 * Ufuncs.pow(mat.sub(graph.placeHolder.getLabel), 2.0).average()
    }
  }

  def loss(pred: Double, label: Double): Double = {
    val diff = pred - label
    0.5 * diff * diff
  }

  override def calGrad(modelOut: Matrix, graph: AngelGraph): Matrix = {
    modelOut.sub(graph.placeHolder.getLabel)
  }

  override def predict(modelOut: Matrix, graph: AngelGraph): Matrix = {
    modelOut match {
      case m: BlasDoubleMatrix =>
        val mat = MFactory.denseDoubleMatrix(m.getNumRows, 3)
        mat.setCol(0, m.getCol(0))
        mat
      case m: BlasFloatMatrix =>
        val mat = MFactory.denseFloatMatrix(m.getNumRows, 3)
        mat.setCol(0, m.getCol(0))
        mat
    }
  }

  override def toString: String = s"L2Loss"
}

class LogLoss extends LossFunc {
  override def calLoss(modelOut: Matrix, graph: AngelGraph): Double = {
    LossFuncs.logloss(modelOut, graph.placeHolder.getLabel).average()
  }

  override def loss(pred: Double, label: Double): Double = {
    Math.log(1 + Math.exp(-pred * label))
  }

  override def calGrad(modelOut: Matrix, graph: AngelGraph): Matrix = {
    LossFuncs.gradlogloss(modelOut, graph.placeHolder.getLabel)
  }

  override def predict(modelOut: Matrix, graph: AngelGraph): Matrix = {
    modelOut match {
      case m: BlasDoubleMatrix =>
        val temp = m.getData
        val data = new Array[Double](temp.length * 3)
        temp.zipWithIndex.foreach { case (value, idx) =>
          data(3 * idx) = value
          data(3 * idx + 1) = 1.0 / (1.0 + Math.exp(-value))
          data(3 * idx + 2) = if (value > 0) 1.0 else -1.0
        }

        MFactory.denseDoubleMatrix(temp.length, 3, data)
      case m: BlasFloatMatrix =>
        val temp = m.getData
        val data = new Array[Float](temp.length * 3)
        temp.zipWithIndex.foreach { case (value, idx) =>
          data(3 * idx) = value
          data(3 * idx + 1) = (1.0 / (1.0 + Math.exp(-value))).toFloat
          data(3 * idx + 2) = if (value > 0) 1.0f else -1.0f
        }

        MFactory.denseFloatMatrix(temp.length, 3, data)
    }
  }

  override def toString: String = s"LogLoss"
}

class HingeLoss extends LossFunc {
  override def calLoss(modelOut: Matrix, graph: AngelGraph): Double = {
    LossFuncs.hingeloss(modelOut, graph.placeHolder.getLabel).average()
  }

  override def loss(pred: Double, label: Double): Double = {
    Math.max(0, 1 - pred * label)
  }

  override def calGrad(modelOut: Matrix, graph: AngelGraph): Matrix = {
    LossFuncs.gradhingeloss(modelOut, graph.placeHolder.getLabel)
  }

  override def predict(modelOut: Matrix, graph: AngelGraph): Matrix = {
    modelOut match {
      case m: BlasDoubleMatrix =>
        val temp = m.getData
        val data = new Array[Double](temp.length * 3)
        temp.zipWithIndex.foreach { case (value, idx) =>
          data(3 * idx) = value
          data(3 * idx + 1) = 1.0 / (1.0 + Math.exp(-2.0 * value))
          data(3 * idx + 2) = if (value > 0) 1.0 else -1.0
        }

        MFactory.denseDoubleMatrix(temp.length, 3, data)
      case m: BlasFloatMatrix =>
        val temp = m.getData
        val data = new Array[Float](temp.length * 3)
        temp.zipWithIndex.foreach { case (value, idx) =>
          data(3 * idx) = value
          data(3 * idx + 1) = (1.0 / (1.0 + Math.exp(-2.0 * value))).toFloat
          data(3 * idx + 2) = if (value > 0) 1.0f else -1.0f
        }

        MFactory.denseFloatMatrix(temp.length, 3, data)
    }
  }

  override def toString: String = s"HingeLoss"
}

class CrossEntropyLoss extends LossFunc {
  val eps = 10e-10

  override def calLoss(modelOut: Matrix, graph: AngelGraph): Double = {
    LossFuncs.entropyloss(modelOut, graph.placeHolder.getLabel).average()
  }

  override def loss(pred: Double, label: Double): Double = {
    if (label > 0) {
      if (pred < eps) {
        -Math.log(eps)
      } else {
        -Math.log(pred)
      }
    } else {
      if (pred > 1 - eps) {
        -Math.log(eps)
      } else {
        -Math.log(1.0 - pred)
      }
    }
  }

  override def calGrad(modelOut: Matrix, graph: AngelGraph): Matrix = {
    LossFuncs.gradentropyloss(modelOut, graph.placeHolder.getLabel)
  }

  override def predict(modelOut: Matrix, graph: AngelGraph): Matrix = {
    modelOut match {
      case m: BlasDoubleMatrix =>
        val temp = m.getData
        val data = new Array[Double](temp.length * 3)
        temp.zipWithIndex.foreach { case (value, idx) =>
          val ord = value / (1 - value)
          if (ord < 0.000001) {
            data(3 * idx) = Math.log(0.000001)
          } else if (ord > Float.MaxValue) {
            data(3 * idx) = Math.log(Float.MaxValue)
          } else {
            data(3 * idx) = Math.log(ord)
          }
          data(3 * idx + 1) = value
          data(3 * idx + 2) = if (value > 0.5) 1.0 else -1.0
        }

        MFactory.denseDoubleMatrix(temp.length, 3, data)
      case m: BlasFloatMatrix =>
        val temp = m.getData
        val data = new Array[Float](temp.length * 3)
        temp.zipWithIndex.foreach { case (value, idx) =>
          val ord = value / (1 - value)
          if (ord < 0.000001) {
            data(3 * idx) = Math.log(0.000001).toFloat
          } else if (ord > Float.MaxValue) {
            data(3 * idx) = Math.log(Float.MaxValue).toFloat
          } else {
            data(3 * idx) = Math.log(ord).toFloat
          }
          data(3 * idx + 1) = value
          data(3 * idx + 2) = if (value > 0.5) 1.0f else -1.0f
        }

        MFactory.denseFloatMatrix(temp.length, 3, data)
    }
  }

  override def toString: String = s"CrossEntropyLoss"
}

class SoftmaxLoss extends LossFunc {
  override def calLoss(modelOut: Matrix, graph: AngelGraph): Double = {
    val mat = Ufuncs.exp(modelOut)
    Ufuncs.idiv(mat, mat.sum(1), true)

    val labels = graph.placeHolder.getLabel.asInstanceOf[BlasFloatMatrix].getData
    var loss: Double = 0
    mat match {
      case m: BlasDoubleMatrix =>
        labels.zipWithIndex.foreach { case (j: Float, i: Int) =>
          loss -= Math.log(m.get(i, j.toInt))
        }
      case m: BlasFloatMatrix =>
        labels.zipWithIndex.foreach { case (j: Float, i: Int) =>
          loss -= Math.log(m.get(i, j.toInt))
        }
    }

    loss / labels.length
  }

  override def calGrad(modelOut: Matrix, graph: AngelGraph): Matrix = {
    val mat = Ufuncs.exp(modelOut)
    Ufuncs.idiv(mat, mat.sum(1), true)

    val labels = graph.placeHolder.getLabel.asInstanceOf[BlasFloatMatrix].getData
    mat match {
      case m: BlasDoubleMatrix =>
        labels.zipWithIndex.foreach { case (j: Float, i: Int) =>
          m.set(i, j.toInt, m.get(i, j.toInt) - 1)
        }
      case m: BlasFloatMatrix =>
        labels.zipWithIndex.foreach { case (j: Float, i: Int) =>
          m.set(i, j.toInt, m.get(i, j.toInt) - 1)
        }
    }

    mat
  }

  def loss(proba: Double, label: Double): Double = -Math.log(proba)

  override def predict(modelOut: Matrix, graph: AngelGraph): Matrix = {
    val mat = Ufuncs.exp(modelOut)
    Ufuncs.idiv(mat, mat.sum(1), true)
    val trueLabels = graph.placeHolder.getLabel.asInstanceOf[BlasFloatMatrix].getData
    val numOutCols: Int = 4

    mat match {
      case m: BlasDoubleMatrix =>
        val labels = m.argmax(1).asInstanceOf[IntDoubleVector]
        val data = new Array[Double](m.getNumRows * numOutCols)
        labels.getStorage.getValues.zipWithIndex.foreach { case (lab, idx) =>
          data(numOutCols * idx) = modelOut.asInstanceOf[BlasDoubleMatrix].get(idx, lab.toInt)
          data(numOutCols * idx + 1) = m.get(idx, lab.toInt)
          data(numOutCols * idx + 2) = if (SharedConf.actionType().equalsIgnoreCase("train") || SharedConf.actionType().equalsIgnoreCase("inctrain")) {
            m.get(idx, trueLabels(idx).toInt)
          } else {
            1e-8
          }
          data(numOutCols * idx + 3) = lab
        }
        MFactory.denseDoubleMatrix(m.getNumRows, numOutCols, data)
      case m: BlasFloatMatrix =>
        val labels = m.argmax(1).asInstanceOf[IntFloatVector]
        val data = new Array[Float](m.getNumRows * numOutCols)
        labels.getStorage.getValues.zipWithIndex.foreach { case (lab, idx) =>
          data(numOutCols * idx) = modelOut.asInstanceOf[BlasFloatMatrix].get(idx, lab.toInt)
          data(numOutCols * idx + 1) = m.get(idx, lab.toInt)
          data(numOutCols * idx + 2) = if (SharedConf.actionType().equalsIgnoreCase("train") || SharedConf.actionType().equalsIgnoreCase("inctrain")) {
            m.get(idx, trueLabels(idx).toInt)
          } else {
            1e-8F
          }
          data(numOutCols * idx + 3) = lab
        }
        MFactory.denseFloatMatrix(m.getNumRows, numOutCols, data)
    }
  }

  override def toString: String = s"SoftmaxLoss"
}

class HuberLoss(delta: Double) extends LossFunc {
  override def calLoss(modelOut: Matrix, graph: AngelGraph): Double = {
    LossFuncs.huberloss(modelOut, graph.placeHolder.getLabel, delta).average()
  }

  override def loss(pred: Double, label: Double): Double = {
    val diff = Math.abs(pred - label)
    if (diff > delta) {
      delta * diff - 0.5 * delta * delta
    } else {
      0.5 * diff * diff
    }
  }

  override def calGrad(modelOut: Matrix, graph: AngelGraph): Matrix = {
    LossFuncs.gradhuberloss(modelOut, graph.placeHolder.getLabel, delta)
  }

  override def predict(modelOut: Matrix, graph: AngelGraph): Matrix = {
    modelOut match {
      case m: BlasDoubleMatrix =>
        val mat = MFactory.denseDoubleMatrix(m.getNumRows, 3)
        mat.setCol(0, m.getCol(0))
        mat
      case m: BlasFloatMatrix =>
        val mat = MFactory.denseFloatMatrix(m.getNumRows, 3)
        mat.setCol(0, m.getCol(0))
        mat
    }
  }

  override def toString: String = s"HuberLoss"

  override def toJson: JObject = {
    (ParamKeys.typeName -> JString(s"${this.getClass.getSimpleName}")) ~ ("delta" -> delta)
  }
}
