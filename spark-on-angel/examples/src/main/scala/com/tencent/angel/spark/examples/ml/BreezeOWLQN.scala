package com.tencent.angel.spark.examples.ml

import scala.collection.mutable.ArrayBuffer

import breeze.linalg.DenseVector
import breeze.optimize.{OWLQN => BrzOWLQN}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD

import com.tencent.angel.spark.examples.util.{Logistic, PSExamples}
import com.tencent.angel.spark.PSClient
import com.tencent.angel.spark.ml.optim.OWLQN
import com.tencent.angel.spark.vector.BreezePSVector

/**
 * There is two ways to update PSVectors in RDD, RemotePSVector and RDDFunction.psAggregate.
 * This is the samples of running Breeze.optimize BreezeOWLQN in spark and two ways of
 * spark-on-angel, respectively.
 */
object BreezeOWLQN {
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

    // run OWLQN
    var startTime = System.currentTimeMillis()
    runOWLQN(trainData, dim, m, maxIter)
    var endTime = System.currentTimeMillis()
    println(s"OWLQN time: ${endTime - startTime}")

    // run PS WLQN
    startTime = System.currentTimeMillis()
    runPsOWLQN(trainData, dim, m, maxIter)
    endTime = System.currentTimeMillis()
    println(s"PS OWLQN time: ${endTime - startTime} ")

    // run PS WLQN
    startTime = System.currentTimeMillis()
    runPsAggregateOWLQN(trainData, dim, m, maxIter)
    endTime = System.currentTimeMillis()
    println(s"PS aggregate OWLQN time: ${endTime - startTime} ")
  }

  def runOWLQN(trainData: RDD[(Vector, Double)], dim: Int, m: Int, maxIter: Int): Unit = {
    val tol = 1e-5
    val initWeight = new DenseVector[Double](dim)
    val l1reg = 0.0
    val owlqn = new BrzOWLQN[Int, DenseVector[Double]](maxIter, m, 0.0, tol)

    val states = owlqn.iterations(Logistic.Cost(trainData), initWeight)

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

  def runPsOWLQN(trainData: RDD[(Vector, Double)], dim: Int, m: Int, maxIter: Int): Unit = {
    val tol = 1e-5
    val pool = PSClient.get.createVectorPool(dim, 20)
    val initWeightPS = pool.createZero().mkBreeze()

    val l1reg = pool.createZero().mkBreeze()
    val owlqn = new OWLQN(maxIter, m, l1reg, tol)
    val states = owlqn.iterations(Logistic.PSCost(trainData), initWeightPS)

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
  }

  def runPsAggregateOWLQN(
      trainData: RDD[(Vector, Double)], dim: Int, m: Int, maxIter: Int): Unit = {
    val tol = 1e-5
    val pool = PSClient.get.createVectorPool(dim, 20)
    val initWeightPS = pool.createZero().mkBreeze()

    val l1reg = pool.createZero().mkBreeze()
    val owlqn = new OWLQN(maxIter, m, l1reg, tol)
    val states = owlqn.iterations(Logistic.PSAggregateCost(trainData), initWeightPS)

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
  }
}
