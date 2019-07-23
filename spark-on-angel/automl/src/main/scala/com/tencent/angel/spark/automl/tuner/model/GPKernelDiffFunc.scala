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


package com.tencent.angel.spark.automl.tuner.model

import breeze.linalg.{MatrixNotSymmetricException, NotConvergedException, DenseMatrix => BDM, DenseVector => BDV}
import breeze.optimize.DiffFunction
import com.tencent.angel.spark.automl.tuner.math.BreezeOp

class GPKernelDiffFunc(model: GPModel) extends DiffFunction[BDV[Double]] {

  var iter: Int = _

  override def calculate(params: BDV[Double]): (Double, BDV[Double]) = {

    try {
      //println(s"------iteration $iter------")
      val covParams = BDV(params.toArray.dropRight(1))
      model.covParams = covParams
      val noiseStdDev = params.toArray.last
      model.noiseStdDev = noiseStdDev
      //println(s"covariance params: $covParams")
      //println(s"standard derivative: $noiseStdDev")

      val meanX = model.meanFunc(model.X)
      val KXX = model.calKXX()

      //println(s"meanX: $meanX")
      //println(s"KXX: $KXX")

      val invKXX = model.calInvKXX(KXX)
      //println("inverse of KXX:")
      //println(invKXX)

      //println("true inverse of KXX:")
      //println(inv(KXX))

      val loglikeLoss = -BreezeOp.logLike(meanX, KXX, invKXX, model.y)
      //println(s"log likelihood loss: $loglikeLoss")

      // calculate partial derivatives
      val covarFuncGrads = model.covFunc.grad(model.X, model.X, covParams)
      //println("covariance grads:")
      //covarFuncGrads.foreach(println)

      val covarNoiseGrad = 2 * noiseStdDev * BDM.eye[Double](model.X.rows)
      //println("covariance noise grads:")
      //println(covarNoiseGrad)

      val allGrads = covarFuncGrads :+ covarNoiseGrad

      val loglikeGrads = BreezeOp.logLikeD(meanX, invKXX, model.y, allGrads).map(d => -d)
      //println(s"grad of covariance params: $loglikeGrads")

      iter = iter + 1

      (loglikeLoss, loglikeGrads)
    } catch {
      case e: NotConvergedException =>
        //println(s"not converge exception $e")
        //(Double.NaN, BDV.zeros[Double](params.size) * Double.NaN)
        throw e
      case e: MatrixNotSymmetricException =>
        println(s"matrix not symmetric exception $e")
        (Double.NaN, BDV.zeros[Double](params.size) * Double.NaN)
        throw e
    }
  }
}
