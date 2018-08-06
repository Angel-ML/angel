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

package com.tencent.angel.spark.examples.ml

import com.tencent.angel.spark.models.vector.enhanced.BreezePSVector
import scala.collection.mutable.ArrayBuffer

import breeze.linalg.DenseVector
import breeze.optimize.StochasticGradientDescent
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.rdd.RDD

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.examples.util.{Logistic, SparkUtils}
import com.tencent.angel.spark.models.vector.PSVector

/**
 * There is two ways to update PSVectors in RDD, RemotePSVector and RDDFunction.psAggregate.
 * This is the samples of running Breeze.optimize SGD in spark and two ways of spark-on-angel,
 * respectively.
 */
object BreezeSGD {

  import SparkUtils._
  def main(args: Array[String]): Unit = {
    parseArgs(args)
    runSpark(this.getClass.getSimpleName) { sc =>
      PSContext.getOrCreate(sc)
      execute(DIM, N, numSlices, ITERATIONS)
      PSContext.stop()
    }
  }

  def execute(
      dim: Int,
      sampleNum: Int,
      partitionNum: Int,
      maxIter: Int,
      stepSize: Double = 0.1): Unit = {

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
    val initWeightPS = PSVector.dense(dim).toBreeze

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
    println(s"weights: ${weight.pull.toDense.values.mkString(" ")}")
  }

  def runPsAggregateSGD(
      trainData: RDD[(Vector, Double)],
      dim: Int,
      stepSize: Double,
      maxIter: Int): Unit = {

    val initWeightPS = PSVector.dense(dim).toBreeze

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
    println(s"weights: ${weight.pull.toDense.values.mkString(" ")}")
  }

}
