package com.tencent.angel.spark.automl

import breeze.linalg.{DenseMatrix, DenseVector}
import breeze.numerics.{cos, pow}
import com.tencent.angel.spark.automl.tuner.kernel.Matern5Iso
import com.tencent.angel.spark.automl.tuner.model.GPModel
import org.junit._

class GPModelTest {

  @Test def testLinear(): Unit = {
    // Test linear: y=2*x
    val X = DenseMatrix((1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0)).t
    val y = 2.0 * DenseVector(1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0)
    val z = DenseMatrix((2.5,4.5,6.5,8.5,10.0,12.0)).t
    val truePredZ = 2.0 * DenseVector(2.5,4.5,6.5,8.5,10.0,12.0)

    val covFunc = Matern5Iso()
    val initCovParams = DenseVector(1.0,1.0)
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

  @Test def testCosine(): Unit = {
    // Test no_linear: y=cos(x)+1
    val X = DenseMatrix((1.0,2.0, 3.0,4.0,5.0,6.0,7.0,8.0,9.0)).t
    val y = cos(DenseVector(1.0,2.0, 3.0,4.0,5.0,6.0,7.0,8.0,9.0))+1.0
    val z = DenseMatrix((2.5, 4.5,6.5,8.5,10.0,12.0)).t
    val truePredZ = cos(DenseVector(2.5, 4.5,6.5,8.5,10.0,12.0))+1.01

    val covFunc = Matern5Iso()
    val initCovParams = DenseVector(1.0,1.0)
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

  @Test def testSquare(): Unit = {
    // Test no_linear: y=x^2
    val X = DenseMatrix((1.0,2.0, 3.0,4.0,5.0,6.0,7.0,8.0,9.0)).t
    val y = DenseVector(1.0,4.0, 9.0,16.0,25.0,36.0,49.0,64.0,81.0)
    val z = DenseMatrix((2.5, 4.5,6.5,8.5,10.0,12.0)).t
    val truePredZ = pow(z,2)

    val covFunc = Matern5Iso()
    val initCovParams = DenseVector(1.0,1.0)
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
