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

import com.tencent.angel.ml.core.PredictResult
import com.tencent.angel.ml.core.network.Graph
import com.tencent.angel.ml.core.network.layers.LossLayer
import com.tencent.angel.ml.core.utils.JsonUtils.{extract, fieldEqualClassName, matchClassName}
import com.tencent.angel.ml.core.utils.LossFuncKeys
import com.tencent.angel.ml.servingmath2.matrix.{BlasDoubleMatrix, BlasFloatMatrix, BlasMatrix, Matrix}
import com.tencent.angel.ml.servingmath2.ufuncs.{LossFuncs, Ufuncs}
import com.tencent.angel.ml.servingmath2.vector.IntDoubleVector
import org.json4s.JsonAST.{JField, JObject, JString, JValue}
import org.json4s.JsonDSL._

import scala.collection.mutable


trait LossFunc extends Serializable {
  var lossLayer: LossLayer = _

  def getLabels: Array[Float] = lossLayer.getLabel.asInstanceOf[BlasFloatMatrix].getData

  def getAttachedArr: Array[String] = lossLayer.getAttached

  def setLossLayer(layer: LossLayer): this.type = {
    lossLayer = layer

    this
  }

  def calLoss(modelOut: Matrix, graph: Graph): Double

  def loss(pred: Double, label: Double): Double

  def calGrad(modelOut: Matrix, graph: Graph): Matrix

  def predict(modelOut: Matrix, graph: Graph): List[PredictResult]

  def toJson: JObject = {
    JObject(JField(LossFuncKeys.typeKey, JString(s"${this.getClass.getSimpleName}")))
  }
}

object LossFunc {
  def fromJson(jast: JValue): LossFunc = {
    jast match {
      case JString(s) if matchClassName[L2Loss](s) =>
        new L2Loss()
      case JString(s) if matchClassName[LogLoss](s) =>
        new LogLoss()
      case JString(s) if matchClassName[HingeLoss](s) =>
        new HingeLoss()
      case JString(s) if matchClassName[CrossEntropyLoss](s) =>
        new CrossEntropyLoss()
      case JString(s) if matchClassName[SoftmaxLoss](s) =>
        new SoftmaxLoss()
      case JString(s) if matchClassName[HuberLoss](s) =>
        new HuberLoss(extract[Double](jast, LossFuncKeys.deltaKey, Some(0.5)).get)
      case obj: JObject if fieldEqualClassName[L2Loss](obj) =>
        new L2Loss()
      case obj: JObject if fieldEqualClassName[LogLoss](obj) =>
        new LogLoss()
      case obj: JObject if fieldEqualClassName[HingeLoss](obj) =>
        new HingeLoss()
      case obj: JObject if fieldEqualClassName[CrossEntropyLoss](obj) =>
        new CrossEntropyLoss()
      case obj: JObject if fieldEqualClassName[SoftmaxLoss](obj) =>
        new SoftmaxLoss()
      case obj: JObject if fieldEqualClassName[HuberLoss](obj) =>
        new HuberLoss(extract[Double](obj, LossFuncKeys.deltaKey, Some(0.5)).get)
      case _ => new LogLoss()
    }
  }

  def getSid(idx: Int, attachedArr: Array[String]): String = {
    if (attachedArr != null) {
      val attached = attachedArr(idx)
      if (attached == null || attached.isEmpty) {
        s"$idx"
      } else {
        attached
      }
    } else {
      s"$idx"
    }
  }
}

class L2Loss extends LossFunc {
  override def calLoss(modelOut: Matrix, graph: Graph): Double = {
    modelOut match {
      case mat: BlasMatrix =>
        0.5 * Ufuncs.pow(mat.sub(lossLayer.getLabel), 2.0).average()
    }
  }

  def loss(pred: Double, label: Double): Double = {
    val diff = pred - label
    0.5 * diff * diff
  }

  override def calGrad(modelOut: Matrix, graph: Graph): Matrix = {
    modelOut.sub(lossLayer.getLabel)
  }

  override def predict(modelOut: Matrix, graph: Graph): List[PredictResult] = {
    val result = new mutable.ListBuffer[PredictResult]()

    val labels = getLabels
    val attachedArr = getAttachedArr
    modelOut match {
      case m: BlasDoubleMatrix =>
        (0 until m.getNumRows).foreach { idx =>
          val pred = m.get(idx, 0)
          val proba = Double.NaN
          val predLabel = pred
          val trueLabel = labels(idx)

          result.append(PredictResult(LossFunc.getSid(idx, attachedArr), pred, proba, predLabel, trueLabel))
        }
      case m: BlasFloatMatrix =>
        (0 until m.getNumRows).foreach { idx =>
          val pred = m.get(idx, 0)
          val proba = Double.NaN
          val predLabel = pred
          val trueLabel = labels(idx)

          result.append(PredictResult(LossFunc.getSid(idx, attachedArr), pred, proba, predLabel, trueLabel))
        }
    }

    result.toList
  }

  override def toString: String = s"L2Loss"
}

class LogLoss extends LossFunc {
  override def calLoss(modelOut: Matrix, graph: Graph): Double = {
    LossFuncs.logloss(modelOut, lossLayer.getLabel).average()
  }

  override def loss(pred: Double, label: Double): Double = {
    Math.log(1 + Math.exp(-pred * label))
  }

  override def calGrad(modelOut: Matrix, graph: Graph): Matrix = {
    LossFuncs.gradlogloss(modelOut, lossLayer.getLabel)
  }

  override def predict(modelOut: Matrix, graph: Graph): List[PredictResult] = {
    val result = new mutable.ListBuffer[PredictResult]()

    val labels = getLabels
    val attachedArr = getAttachedArr
    modelOut match {
      case m: BlasDoubleMatrix =>
        m.getData.zipWithIndex.foreach { case (pred, idx) =>
          val proba = 1.0 / (1.0 + Math.exp(-pred))
          val predLabel = if (pred > 0) 1.0 else -1.0
          val trueLabel = labels(idx)

          result.append(PredictResult(LossFunc.getSid(idx, attachedArr), pred, proba, predLabel, trueLabel))
        }
      case m: BlasFloatMatrix =>
        m.getData.zipWithIndex.foreach { case (pred, idx) =>
          val proba = 1.0 / (1.0 + Math.exp(-pred))
          val predLabel = if (pred > 0) 1.0 else -1.0
          val trueLabel = labels(idx)

          result.append(PredictResult(LossFunc.getSid(idx, attachedArr), pred, proba, predLabel, trueLabel))
        }
    }

    result.toList
  }

  override def toString: String = s"LogLoss"
}

class HingeLoss extends LossFunc {
  override def calLoss(modelOut: Matrix, graph: Graph): Double = {
    LossFuncs.hingeloss(modelOut, lossLayer.getLabel).average()
  }

  override def loss(pred: Double, label: Double): Double = {
    Math.max(0, 1 - pred * label)
  }

  override def calGrad(modelOut: Matrix, graph: Graph): Matrix = {
    LossFuncs.gradhingeloss(modelOut, lossLayer.getLabel)
  }

  override def predict(modelOut: Matrix, graph: Graph): List[PredictResult] = {
    val result = new mutable.ListBuffer[PredictResult]()

    val labels = getLabels
    val attachedArr = getAttachedArr
    modelOut match {
      case m: BlasDoubleMatrix =>
        m.getData.zipWithIndex.foreach { case (pred, idx) =>
          val proba = 1.0 / (1.0 + Math.exp(-pred))
          val predLabel = if (pred > 0) 1.0 else -1.0
          val trueLabel = labels(idx)

          result.append(PredictResult(LossFunc.getSid(idx, attachedArr), pred, proba, predLabel, trueLabel))
        }
      case m: BlasFloatMatrix =>
        m.getData.zipWithIndex.foreach { case (pred, idx) =>
          val proba = 1.0 / (1.0 + Math.exp(-pred))
          val predLabel = if (pred > 0) 1.0 else -1.0
          val trueLabel = labels(idx)

          result.append(PredictResult(LossFunc.getSid(idx, attachedArr), pred, proba, predLabel, trueLabel))
        }
    }

    result.toList
  }

  override def toString: String = s"HingeLoss"
}

class CrossEntropyLoss extends LossFunc {
  val eps = 10e-10

  override def calLoss(modelOut: Matrix, graph: Graph): Double = {
    LossFuncs.entropyloss(modelOut, lossLayer.getLabel).average()
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

  override def calGrad(modelOut: Matrix, graph: Graph): Matrix = {
    LossFuncs.gradentropyloss(modelOut, lossLayer.getLabel)
  }

  override def predict(modelOut: Matrix, graph: Graph): List[PredictResult] = {
    val result = new mutable.ListBuffer[PredictResult]()

    val labels = getLabels
    val attachedArr = getAttachedArr
    modelOut match {
      case m: BlasDoubleMatrix =>
        m.getData.zipWithIndex.foreach { case (proba, idx) =>

          val ord = proba / (1 - proba)
          val pred = if (ord < 0.000001) {
            Math.log(0.000001)
          } else if (ord > Float.MaxValue) {
            Math.log(Float.MaxValue)
          } else {
            Math.log(ord)
          }
          val predLabel = if (proba >= 0.5) 1.0 else -1.0
          val trueLabel = labels(idx)

          result.append(PredictResult(LossFunc.getSid(idx, attachedArr), pred, proba, predLabel, trueLabel))
        }
      case m: BlasFloatMatrix =>
        m.getData.zipWithIndex.foreach { case (proba, idx) =>
          val ord = proba / (1 - proba)
          val pred = if (ord < 0.000001) {
            Math.log(0.000001)
          } else if (ord > Float.MaxValue) {
            Math.log(Float.MaxValue)
          } else {
            Math.log(ord)
          }
          val predLabel = if (proba >= 0.5) 1.0 else -1.0
          val trueLabel = labels(idx)

          result.append(PredictResult(LossFunc.getSid(idx, attachedArr), pred, proba, predLabel, trueLabel))
        }
    }

    result.toList
  }

  override def toString: String = s"CrossEntropyLoss"
}

class SoftmaxLoss extends LossFunc {
  override def calLoss(modelOut: Matrix, graph: Graph): Double = {
    val mat = Ufuncs.exp(modelOut)
    Ufuncs.idiv(mat, mat.sum(1), true)

    val labels = getLabels
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

  override def calGrad(modelOut: Matrix, graph: Graph): Matrix = {
    val mat = Ufuncs.exp(modelOut)
    Ufuncs.idiv(mat, mat.sum(1), true)

    mat match {
      case m: BlasDoubleMatrix =>
        getLabels.zipWithIndex.foreach { case (j: Float, i: Int) =>
          m.set(i, j.toInt, m.get(i, j.toInt) - 1)
        }
      case m: BlasFloatMatrix =>
        getLabels.zipWithIndex.foreach { case (j: Float, i: Int) =>
          m.set(i, j.toInt, m.get(i, j.toInt) - 1)
        }
    }

    mat
  }

  def loss(proba: Double, label: Double): Double = -Math.log(proba)

  override def predict(modelOut: Matrix, graph: Graph): List[PredictResult] = {
    val result = new mutable.ListBuffer[PredictResult]()

    val mat = Ufuncs.exp(modelOut)
    Ufuncs.idiv(mat, mat.sum(1), true)
    val labels = getLabels
    val attachedArr = getAttachedArr
    mat match {
      case m: BlasDoubleMatrix =>
        val predlabels = m.argmax(1).asInstanceOf[IntDoubleVector]
        predlabels.getStorage.getValues.zipWithIndex.foreach { case (label, idx) =>
          val pred = modelOut.asInstanceOf[BlasDoubleMatrix].get(idx, label.toInt)
          val proba = m.get(idx, label.toInt)
          val attached = m.get(idx, labels(idx).toInt)
          val trueLabel = labels(idx)

          result.append(PredictResult(LossFunc.getSid(idx, attachedArr), pred, proba, label, trueLabel, attached))
        }
      case m: BlasFloatMatrix =>
        val predlabels = m.argmax(1).asInstanceOf[IntDoubleVector]
        predlabels.getStorage.getValues.zipWithIndex.foreach { case (label, idx) =>
          val pred = modelOut.asInstanceOf[BlasDoubleMatrix].get(idx, label.toInt)
          val proba = m.get(idx, label.toInt)
          val attached = m.get(idx, labels(idx).toInt)
          val trueLabel = labels(idx)

          result.append(PredictResult(LossFunc.getSid(idx, attachedArr), pred, proba, label, trueLabel, attached))
        }
    }

    result.toList
  }

  override def toString: String = s"SoftmaxLoss"
}

class HuberLoss(delta: Double) extends LossFunc {
  override def calLoss(modelOut: Matrix, graph: Graph): Double = {
    LossFuncs.huberloss(modelOut, lossLayer.getLabel, delta).average()
  }

  override def loss(pred: Double, label: Double): Double = {
    val diff = Math.abs(pred - label)
    if (diff > delta) {
      delta * diff - 0.5 * delta * delta
    } else {
      0.5 * diff * diff
    }
  }

  override def calGrad(modelOut: Matrix, graph: Graph): Matrix = {
    LossFuncs.gradhuberloss(modelOut, lossLayer.getLabel, delta)
  }

  override def predict(modelOut: Matrix, graph: Graph): List[PredictResult] = {
    val result = new mutable.ListBuffer[PredictResult]()

    val labels = getLabels
    val attachedArr = getAttachedArr
    modelOut match {
      case m: BlasDoubleMatrix =>
        (0 until m.getNumRows).foreach { idx =>
          val pred = m.get(idx, 0)
          val proba = Double.NaN
          val predLabel = pred
          val trueLabel = labels(idx)

          result.append(PredictResult(LossFunc.getSid(idx, attachedArr), pred, proba, predLabel, trueLabel))
        }
      case m: BlasFloatMatrix =>
        (0 until m.getNumRows).foreach { idx =>
          val pred = m.get(idx, 0)
          val proba = Double.NaN
          val predLabel = pred
          val trueLabel = labels(idx)

          result.append(PredictResult(LossFunc.getSid(idx, attachedArr), pred, proba, predLabel, trueLabel))
        }
    }

    result.toList
  }

  override def toString: String = s"HuberLoss"

  override def toJson: JObject = {
    (LossFuncKeys.typeKey -> JString(s"${this.getClass.getSimpleName}")) ~ ("delta" -> delta)
  }
}
