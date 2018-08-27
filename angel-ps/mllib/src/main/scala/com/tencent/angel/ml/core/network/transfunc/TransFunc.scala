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


package com.tencent.angel.ml.core.network.transfunc


import com.tencent.angel.ml.math2.matrix.Matrix
import com.tencent.angel.ml.math2.ufuncs.{TransFuncs, Ufuncs}

trait TransFunc extends Serializable {
  def apply(mat: Matrix): Matrix

  def calGrad(output: Matrix, grad: Matrix): Matrix
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
