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

import breeze.linalg.{Axis, MatrixNotSymmetricException, cholesky, diag, DenseMatrix => BDM, DenseVector => BDV}
import breeze.optimize.{AdaDeltaGradientDescent, LBFGS, StochasticGradientDescent}
import com.tencent.angel.spark.automl.tuner.kernel.{Covariance, CovarianceType}
import com.tencent.angel.spark.automl.tuner.math.BreezeOp

import scala.math._

class GPModel(val covFunc: Covariance,
              var covParams: BDV[Double],
              var noiseStdDev: Double,
              val meanFunc: (BDM[Double]) => BDV[Double]) {

  var X: BDM[Double] = _
  var y: BDV[Double] = _
  var KXX: BDM[Double] = _
  var L: BDM[Double] = _

  def remove(idx: Int): Unit = {
    X
  }

  def fit(newX: BDM[Double],
          newy: BDV[Double]): Boolean = {
    require(newX.rows == newy.length, "incompatible size of the input X and y")

    var trainSuccess = true

    if ((X == null && y == null) ||
      (newX.rows > X.rows && newy.length > y.length)) {
      X = newX
      y = newy
    }

    val kernelDiffFunc = new GPKernelDiffFunc(this)
    val initParams = BDV(covParams.toArray :+ noiseStdDev)
    //println(s"init params: ${initParams}")

    var newParams = initParams
    val optimizer = new LBFGS[BDV[Double]](maxIter = 10, m = 7, tolerance = 1e-10)
    //val optimizer = new SimpleSGD[BDV[Double]](1, 10)
    try {
      newParams = optimizer.minimize(kernelDiffFunc, initParams)
    } catch {
      case _: breeze.linalg.NotConvergedException | _ : MatrixNotSymmetricException =>
        //println(s"Breeze Not Converged Exception")
        newParams = initParams
        trainSuccess = false
        X = X.delete(X.rows - 1, Axis._0)
        y = y.slice(0, y.length - 1)
    }

//    println(optimizer)
//    println(s"new params: ${newParams}")
//    if(!checkParam(newParams)) {
//      newParams = initParams
//      println(s"reset to init params: ${newParams}")
//      trainSuccess = false
//      println(s"history size: ${X.rows} ${y.length}")
//      X = X.delete(X.rows - 1, Axis._0)
//      y = y.slice(0, y.length - 1)
//      println(s"history size: ${X.rows} ${y.length}")
//    }

    val newCovParams = BDV(newParams.toArray.dropRight(1))
    val newNoiseStdDev = newParams.toArray.last

    this.covParams = newCovParams
    this.noiseStdDev = newNoiseStdDev

    trainSuccess
  }

  def checkParam(params: BDV[Double]): Boolean = {
    var isValid = true
    params.values.foreach{ param: Double =>
      if(param.isNaN || param.isInfinity)
        isValid = false
    }
    isValid
  }

  def update(newX: BDM[Double],
             newy: BDV[Double]): this.type = {
    this
  }

  def predict(newX: BDM[Double]): BDM[Double] = {
    if (X == null || y == null) {
      BDM.zeros(newX.rows, cols = 2)
    } else {
      val meanX = meanFunc(X)

      val KXX = calKXX()

      val invKXX = calInvKXX(KXX)

      val KXZ = covFunc.cov(X, newX, covParams)

      val KZZ = covFunc.cov(newX, newX, covParams)

      val meanNewX = meanFunc(newX)

      val predMean = meanNewX + KXZ.t * (invKXX * (y - meanX))
      val predVar = diag(KZZ - KXZ.t * invKXX * KXZ).map { v =>
        if (v < -1e-12 | v.isNaN | v.isInfinite) 0 else v
      }

      BDV.horzcat(predMean, predVar)
    }
  }

  def calKXX(): BDM[Double] = {
    val KXX = covFunc.cov(X, X, covParams) +
      pow(noiseStdDev, 2) * BDM.eye[Double](X.rows)
    //+ BDM.eye[Double](X.rows) * 1e-7

    KXX
  }

  def calInvKXX(KXX: BDM[Double]): BDM[Double] = {
    val l = cholesky(KXX)
    val invKXX = BreezeOp.choleskyInv(l.t)

    invKXX
  }
}

object GPModel {

  def apply(covFunc: Covariance,
            covParams: BDV[Double],
            noiseStdDev: Double,
            meanFunc: (BDM[Double]) => BDV[Double]): GPModel = {
    new GPModel(covFunc, covParams, noiseStdDev, meanFunc)
  }

  def apply(covFunc: Covariance,
            covParams: BDV[Double],
            noiseStdDev: Double,
            mean: Double = 0.0): GPModel = {
    val meanFunc = (x: BDM[Double]) => BDV.zeros[Double](x.rows) + mean
    new GPModel(covFunc, covParams, noiseStdDev, meanFunc)
  }

  def apply(covName: String,
            covParams: BDV[Double],
            noiseStdDev: Double,
            meanFunc: (BDM[Double]) => BDV[Double]): GPModel = {
    new GPModel(CovarianceType.fromString(covName), covParams, noiseStdDev, meanFunc)
  }

  def apply(covType: CovarianceType.Value,
            covParams: BDV[Double],
            noiseStdDev: Double,
            meanFunc: (BDM[Double]) => BDV[Double]): GPModel = {
    new GPModel(CovarianceType.fromString(covType), covParams, noiseStdDev, meanFunc)
  }
}
