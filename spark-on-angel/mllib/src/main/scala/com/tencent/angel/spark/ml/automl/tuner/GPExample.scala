package com.tencent.angel.spark.ml.automl.tuner

import breeze.linalg._
import breeze.numerics._
import com.tencent.angel.spark.ml.automl.tuner.kernel.{Matern5Iso, SquareExpIso}
import com.tencent.angel.spark.ml.automl.tuner.model.GPModel

object GPExample {

  def main(args: Array[String]): Unit = {

    val X = DenseMatrix((1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0)).t
    val y = 2.0 * DenseVector(1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0)
    val z = DenseMatrix((2.5,4.5,6.5,8.5,10.0,12.0)).t
    val truePredZ = 2.0 * DenseVector(2.5,4.5,6.5,8.5,10.0,12.0)

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
    val initCovParams = DenseVector(1.0,1.0)
    val initNoiseStdDev = 0.1

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
