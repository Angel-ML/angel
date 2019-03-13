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

import breeze.linalg.{DenseMatrix, DenseVector}
import com.tencent.angel.spark.automl.tuner.kernel.Matern5Iso

object GPExample {

  def main(args: Array[String]): Unit = {

    val X = DenseMatrix((1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0)).t
    val y = 2.0 * DenseVector(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0)
    val z = DenseMatrix((2.5, 4.5, 6.5, 8.5, 10.0, 12.0)).t
    val truePredZ = 2.0 * DenseVector(2.5, 4.5, 6.5, 8.5, 10.0, 12.0)

    //  //2.Test no_linear(y=cos(x)+1)
    //  val X = DenseMatrix((1.0,2.0, 3.0,4.0,5.0,6.0,7.0,8.0,9.0)).t
    //  val y = cos(DenseVector(1.0,2.0, 3.0,4.0,5.0,6.0,7.0,8.0,9.0))+1.0
    //  val z = DenseMatrix((2.5, 4.5,6.5,8.5,10.0,12.0)).t
    //  val truePredZ = cos(DenseVector(2.5, 4.5,6.5,8.5,10.0,12.0))+1.0

    //  //3.Test no_linear(y=x^2)
    //  val X = DenseMatrix((1.0,2.0, 3.0,4.0,5.0,6.0,7.0,8.0,9.0)).t
    //  val y = DenseVector(1.0,4.0, 9.0,16.0,25.0,36.0,49.0,64.0,81.0)
    //  val z = DenseMatrix((2.5, 4.5,6.5,8.5,10.0,12.0)).t
    //  val truePredZ = pow(z,2)

    //val covFunc = SquareExpIso()
    val covFunc = Matern5Iso()
    val initCovParams = DenseVector(1.0, 1.0)
    val initNoiseStdDev = 0.01

    val gpModel = GPModel(covFunc, initCovParams, initNoiseStdDev)

    gpModel.fit(X, y)

    println("Fitted covariance function params:")
    println(gpModel.covParams)
    println("Fitted noiseStdDev:")
    println(gpModel.noiseStdDev)
    println("\n")

    val prediction = gpModel.predict(z)
    println("Mean and Var:")
    println(prediction)
    println("True value:")
    println(truePredZ)
  }

}
