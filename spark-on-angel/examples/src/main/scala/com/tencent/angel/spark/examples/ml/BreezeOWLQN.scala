/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.models.vector.enhanced.BreezePSVector
import scala.collection.mutable.ArrayBuffer

import breeze.linalg.DenseVector
import breeze.optimize.{OWLQN => BrzOWLQN}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.examples.util.{Logistic, SparkUtils}
import com.tencent.angel.spark.ml.optimize.OWLQN
import com.tencent.angel.spark.models.vector.PSVector

/**
 * There is two ways to update PSVectors in RDD, RemotePSVector and RDDFunction.psAggregate.
 * This is the samples of running Breeze.optimize BreezeOWLQN in spark and two ways of
 * spark-on-angel, respectively.
 */
object BreezeOWLQN {
  import SparkUtils._
  def main(args: Array[String]): Unit = {
    parseArgs(args)
    runSpark(this.getClass.getSimpleName) { sc =>
      PSContext.getOrCreate(sc)
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

    val initWeightPS = PSVector.dense(dim, 10 * m).toBreeze
    val l1reg = PSVector.duplicate(initWeightPS.component).toBreeze

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
    println(s"weights: ${weight.pull.toDense.values.mkString(" ")}")
  }

  def runPsAggregateOWLQN(
      trainData: RDD[(Vector, Double)], dim: Int, m: Int, maxIter: Int): Unit = {
    val tol = 1e-5
    val initWeightPS = PSVector.dense(dim, 10 * m).toBreeze

    val l1reg = PSVector.duplicate(initWeightPS.component).toBreeze
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
    println(s"weights: ${weight.pull.toDense.values.mkString(" ")}")
  }
}
