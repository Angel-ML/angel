package com.tencent.angel.spark.examples.ml

import scala.collection.mutable.ArrayBuffer

import breeze.linalg.DenseVector
import breeze.optimize.LBFGS
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD

import com.tencent.angel.spark.examples.util.{Logistic, PSExamples}
import com.tencent.angel.spark.PSClient
import com.tencent.angel.spark.vector.BreezePSVector

/**
 * There is two ways to update PSVectors in RDD, RemotePSVector and RDDFunction.psAggregate.
 * This is the samples of running Breeze.optimize LBFGS in spark and two ways of spark-on-angel,
 * respectively.
 */
object BreezeLBFGS {
  import PSExamples._
  def main(args: Array[String]): Unit = {
    parseArgs(args)
    runWithSparkContext(this.getClass.getSimpleName) { sc =>
      PSClient.setup(sc)
      execute(DIM, N, numSlices, ITERATIONS)
    }
  }
  def execute(dim: Int, sampleNum: Int, partitionNum: Int, maxIter: Int, m: Int = 10): Unit = {

    val trainData = Logistic.generateLRData(sampleNum, dim, partitionNum)

    // run LBFGS
    var startTime = System.currentTimeMillis()
    runLBFGS(trainData, dim, m, maxIter)
    var endTime = System.currentTimeMillis()
    println(s"LBFGS time: ${endTime - startTime}")

    // run PS LBFGS
    startTime = System.currentTimeMillis()
    runPsLBFGS(trainData, dim, m, maxIter)
    endTime = System.currentTimeMillis()
    println(s"PS LBFGS time: ${endTime - startTime} ")

    // run PS aggregate LBFGS
    startTime = System.currentTimeMillis()
    runPsAggregateLBFGS(trainData, dim, m, maxIter)
    endTime = System.currentTimeMillis()
    println(s"PS aggregate LBFGS time: ${endTime - startTime} ")
  }

   def runLBFGS(trainData: RDD[(Vector, Double)], dim: Int, m: Int, maxIter: Int): Unit = {
    val tol = 1e-5
    val initWeight = new DenseVector[Double](dim)
    val lbfgs = new LBFGS[DenseVector[Double]](maxIter, m, tol)
    val states = lbfgs.iterations(Logistic.Cost(trainData), initWeight)

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

  def runPsLBFGS(trainData: RDD[(Vector, Double)], dim: Int, m: Int, maxIter: Int): Unit = {
    val tol = 1e-5
    val pool = PSClient.get.createVectorPool(dim, 20)
    val initWeightPS = pool.createZero().mkBreeze()
    val lbfgs = new LBFGS[BreezePSVector](maxIter, m, tol)
    val states = lbfgs.iterations(Logistic.PSCost(trainData), initWeightPS)

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

  def runPsAggregateLBFGS(
      trainData: RDD[(Vector, Double)], dim: Int, m: Int, maxIter: Int): Unit = {
    val tol = 1e-5
    val pool = PSClient.get.createVectorPool(dim, 20)
    val initWeightPS = pool.createZero().mkBreeze()
    val lbfgs = new LBFGS[BreezePSVector](maxIter, m, tol)
    val states = lbfgs.iterations(Logistic.PSAggregateCost(trainData), initWeightPS)

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
