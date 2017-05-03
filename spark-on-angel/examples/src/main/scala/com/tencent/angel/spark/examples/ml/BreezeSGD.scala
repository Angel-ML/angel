package com.tencent.angel.spark.examples.ml

import scala.collection.mutable.ArrayBuffer

import breeze.linalg.DenseVector
import breeze.optimize.StochasticGradientDescent
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD

import com.tencent.angel.spark.examples.util.{Logistic, PSExamples}
import com.tencent.angel.spark.PSClient
import com.tencent.angel.spark.vector.BreezePSVector

/**
 * There is two ways to update PSVectors in RDD, RemotePSVector and RDDFunction.psAggregate.
 * This is the samples of running Breeze.optimize SGD in spark and two ways of spark-on-angel,
 * respectively.
 */
object BreezeSGD {

  import PSExamples._
  def main(args: Array[String]): Unit = {
    parseArgs(args)
    runWithSparkContext(this.getClass.getSimpleName) { sc =>
      PSClient.setup(sc)
      execute(DIM, N, numSlices, ITERATIONS)
    }
  }

  def execute(
      dim: Int,
      sampleNum: Int,
      partitionNum: Int,
      maxIter: Int, stepSize:
      Double = 0.1): Unit = {

    val trainData = Logistic.generateLRData(sampleNum, dim, partitionNum)

    // runSGD
    var startTime = System.currentTimeMillis()
    runSGD(trainData, dim, stepSize, maxIter)
    var endTime = System.currentTimeMillis()
    println(s"SGD time: ${endTime - startTime}")

    // run PS SGD
    startTime = System.currentTimeMillis()
    runPsSGD(trainData, dim, stepSize, maxIter)
    endTime = System.currentTimeMillis()
    println(s"PS SGD time: ${endTime - startTime} ")

    // run PS aggregate SGD
    startTime = System.currentTimeMillis()
    runPsAggregateSGD(trainData, dim, stepSize, maxIter)
    endTime = System.currentTimeMillis()
    println(s"PS aggregate SGD time: ${endTime - startTime} ")
  }

  def runSGD(trainData: RDD[(Vector, Double)], dim: Int, stepSize: Double, maxIter: Int): Unit = {
    val initWeight = new DenseVector[Double](dim)
    val sgd = StochasticGradientDescent[DenseVector[Double]](stepSize, maxIter)
    val states = sgd.iterations(Logistic.Cost(trainData), initWeight)

    val lossHistory = new ArrayBuffer[Double]()
    var weight = new DenseVector[Double](dim)
    while (states.hasNext) {
      val state = states.next()
      lossHistory += state.value

      if (!states.hasNext) {
        weight = state.x
      }
    }
    println(s"loss history: ${lossHistory.toArray.mkString(" ")}")
    println(s"weights: ${weight.toArray.mkString(" ")}")
  }

  def runPsSGD(trainData: RDD[(Vector, Double)], dim: Int, stepSize: Double, maxIter: Int): Unit = {
    val pool = PSClient.get.createVectorPool(dim, 10)
    val initWeightPS = pool.createZero().mkBreeze()
    val sgd = StochasticGradientDescent[BreezePSVector](stepSize, maxIter)
    val states = sgd.iterations(Logistic.PSCost(trainData), initWeightPS)

    val lossHistory = new ArrayBuffer[Double]()
    var weight: BreezePSVector = null
    while (states.hasNext) {
      val state = states.next()
      lossHistory += state.value

      if (!states.hasNext) {
        weight = state.x
      }
    }
    println(s"loss history: ${lossHistory.toArray.mkString(" ")}")
    println(s"weights: ${weight.toLocal.get().mkString(" ")}")
    PSClient.get.destroyVectorPool(pool)
  }

  def runPsAggregateSGD(
      trainData: RDD[(Vector, Double)],
      dim: Int,
      stepSize: Double,
      maxIter: Int): Unit = {

    val pool = PSClient.get.createVectorPool(dim, 10)
    val initWeightPS = pool.createZero().mkBreeze()
    val sgd = StochasticGradientDescent[BreezePSVector](stepSize, maxIter)
    val states = sgd.iterations(Logistic.PSAggregateCost(trainData), initWeightPS)

    val lossHistory = new ArrayBuffer[Double]()
    var weight: BreezePSVector = null
    while (states.hasNext) {
      val state = states.next()
      lossHistory += state.value

      if (!states.hasNext) {
        weight = state.x
      }
    }
    println(s"loss history: ${lossHistory.toArray.mkString(" ")}")
    println(s"weights: ${weight.toLocal.get().mkString(" ")}")
    PSClient.get.destroyVectorPool(pool)
  }

}
