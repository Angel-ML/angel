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


package com.tencent.angel.ml.core.network

import com.tencent.angel.ml.core.utils.{MLException, TransFuncKeys}
import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.ufuncs.{TransFuncs, Ufuncs}
import org.json4s.JsonAST.{JField, JObject, JString, JValue}
import org.json4s.JsonDSL._
import com.tencent.angel.ml.core.utils.JsonUtils.extract
import com.tencent.angel.ml.core.utils.JsonUtils.{matchClassName, fieldEqualClassName}


trait TransFunc extends Serializable {
  def apply(mat: Matrix): Matrix

  def calGrad(output: Matrix, grad: Matrix): Matrix

  def toJson: JObject = {
    JObject(JField(TransFuncKeys.typeKey, s"${this.getClass.getSimpleName}"))
  }
}

object TransFunc {
  def fromString(name: String): TransFunc = {
    name match {
      case s: String if matchClassName[Identity](s) =>
        new Identity()
      case s: String if matchClassName[Sigmoid](s) =>
        new Sigmoid()
      case s: String if matchClassName[Relu](s) =>
        new Relu()
      case s: String if matchClassName[Softmax](s) =>
        new Softmax()
      case s: String if matchClassName[Tanh](s) =>
        new Tanh()
      case s: String if matchClassName[SigmoidWithDropout](s) =>
        new SigmoidWithDropout(0.5, "Train")
      case s: String if matchClassName[TanhWithDropout](s) =>
        new TanhWithDropout(0.5, "Train")
      case s: String if matchClassName[Dropout](s) =>
        new Dropout(0.5, "Train")
    }
  }

  def defaultTransFunc(): TransFunc = {
    new Relu()
  }

  def fromJson(jast: JValue): TransFunc = {
    jast match {
      case JString(s) if matchClassName[Identity](s) =>
        new Identity()
      case JString(s) if matchClassName[Sigmoid](s) =>
        new Sigmoid()
      case JString(s) if matchClassName[Relu](s) =>
        new Relu()
      case JString(s) if matchClassName[Softmax](s) =>
        new Softmax()
      case JString(s) if matchClassName[Tanh](s) =>
        new Tanh()
      case JString(s) if matchClassName[SigmoidWithDropout](s) =>
        new SigmoidWithDropout(0.5, "Train")
      case JString(s) if matchClassName[TanhWithDropout](s) =>
        new TanhWithDropout(0.5, "Train")
      case JString(s) if matchClassName[Dropout](s) =>
        new Dropout(0.5, "Train")
      case obj: JObject if fieldEqualClassName[Identity](obj) =>
        new Identity()
      case obj: JObject if fieldEqualClassName[Sigmoid](obj) =>
        new Sigmoid()
      case obj: JObject if fieldEqualClassName[Relu](obj) =>
        new Relu()
      case obj: JObject if fieldEqualClassName[Softmax](obj) =>
        new Softmax()
      case obj: JObject if fieldEqualClassName[Tanh](obj) =>
        new Tanh()
      case obj: JObject if fieldEqualClassName[SigmoidWithDropout](obj) =>
        new SigmoidWithDropout(
          extract[Double](obj, TransFuncKeys.probaKey, Some(0.5)).get,
          extract[String](obj, TransFuncKeys.actionTypeKey, Some("Train")).get
        )
      case obj: JObject if fieldEqualClassName[TanhWithDropout](obj) =>
        new TanhWithDropout(
          extract[Double](obj, TransFuncKeys.probaKey, Some(0.5)).get,
          extract[String](obj, TransFuncKeys.actionTypeKey, Some("Train")).get
        )
      case obj: JObject if fieldEqualClassName[Dropout](obj) =>
        new Dropout(
          extract[Double](obj, TransFuncKeys.probaKey, Some(0.5)).get,
          extract[String](obj, TransFuncKeys.actionTypeKey, Some("Train")).get
        )
      case _ => new Relu()
    }
  }

  def defaultJson: JObject = {
    JObject(JField(TransFuncKeys.typeKey, s"${classOf[Relu].getSimpleName}"))
  }
}

class Identity() extends TransFunc {
  def apply(mat: Matrix): Matrix = mat

  def calGrad(output: Matrix, grad: Matrix): Matrix = grad
}

class Sigmoid() extends TransFunc {
  def apply(mat: Matrix): Matrix = {
    TransFuncs.sigmoid(mat)
  }

  def calGrad(output: Matrix, grad: Matrix): Matrix = {
    TransFuncs.gradsigmoid(output, grad)
  }
}

class Relu() extends TransFunc {
  def apply(mat: Matrix): Matrix = {
    TransFuncs.relu(mat)
  }

  override def calGrad(output: Matrix, grad: Matrix): Matrix = {
    TransFuncs.gradrelu(output, grad)
  }
}

class Tanh() extends TransFunc {
  def apply(mat: Matrix): Matrix = {
    TransFuncs.tanh(mat)
  }

  def calGrad(output: Matrix, grad: Matrix): Matrix = {
    TransFuncs.gradtanh(output, grad)
  }
}

class SigmoidWithDropout(proba: Double, actionType: String) extends TransFunc {
  def apply(mat: Matrix): Matrix = {
    actionType match {
      case "train" => TransFuncs.sigmoidwithdropout(mat, proba)
      case "predict" => TransFuncs.sigmoid(mat).imul(1 - proba)
    }
  }

  def calGrad(output: Matrix, grad: Matrix): Matrix = {
    TransFuncs.gradsigmoidwithdropout(output, grad)
  }

  override def toJson: JObject = {
    (TransFuncKeys.typeKey -> s"${this.getClass.getSimpleName}") ~
      (TransFuncKeys.probaKey -> proba) ~ (TransFuncKeys.actionTypeKey -> actionType)
  }
}

class TanhWithDropout(proba: Double, actionType: String) extends TransFunc {
  def apply(mat: Matrix): Matrix = {
    actionType match {
      case "train" => TransFuncs.tanhwithdropout(mat, proba)
      case "predict" => TransFuncs.tanh(mat).imul(1 - proba)
    }
  }

  override def calGrad(output: Matrix, grad: Matrix): Matrix = {
    TransFuncs.gradtanhwithdropout(output, grad)
  }

  override def toJson: JObject = {
    (TransFuncKeys.typeKey -> s"${this.getClass.getSimpleName}") ~
      (TransFuncKeys.probaKey -> proba) ~ (TransFuncKeys.actionTypeKey -> actionType)
  }
}

class Dropout(proba: Double, actionType: String) extends TransFunc {
  def apply(mat: Matrix): Matrix = {
    actionType match {
      case "train" => TransFuncs.dropout(mat, proba)
      case "predict" => mat.mul(1 - proba)
    }
  }

  override def calGrad(output: Matrix, grad: Matrix): Matrix = {
    TransFuncs.graddropout(output, grad, proba)
  }

  override def toJson: JObject = {
    (TransFuncKeys.typeKey -> s"${this.getClass.getSimpleName}") ~
      (TransFuncKeys.probaKey -> proba) ~ (TransFuncKeys.actionTypeKey -> actionType)
  }
}

class Softmax() extends TransFunc {
  override def apply(mat: Matrix): Matrix = {
    val expMat = Ufuncs.exp(mat)
    Ufuncs.idiv(expMat, expMat.sum(1), true)
  }

  override def calGrad(output: Matrix, grad: Matrix): Matrix = {
    Ufuncs.sub(grad, output.mul(grad).sum(1), true).imul(output)
  }
}
