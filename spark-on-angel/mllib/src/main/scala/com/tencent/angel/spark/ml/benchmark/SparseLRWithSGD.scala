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
 */

package com.tencent.angel.spark.ml.benchmark

import scala.collection.mutable.ArrayBuffer

import breeze.linalg.DenseVector
import breeze.optimize.StochasticGradientDescent
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.linalg.OneHotVector
import com.tencent.angel.spark.ml.sparse.SparseLogistic
import com.tencent.angel.spark.ml.util.{ArgsUtil, DataLoader}
import com.tencent.angel.spark.models.vector.PSVector
import com.tencent.angel.spark.models.vector.enhanced.BreezePSVector


object SparseLRWithSGD {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params("input")
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val stepSize = params.getOrElse("stepSize", "1.0").toDouble
    val maxIter = params.getOrElse("maxIter", "50").toInt
    val updateType = params.getOrElse("updateType", "spark")  // spark or ps

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master(mode)
      .getOrCreate()

    if (updateType == "ps") PSContext.getOrCreate(spark.sparkContext)

    val tempInstances = DataLoader.loadOneHotInstance(input, partitionNum, sampleRate, -1).rdd
      .map { row =>
        Tuple2(row.getAs[scala.collection.mutable.WrappedArray[Long]](1).toArray, row.getString(0).toDouble)
      }
    val featLength = tempInstances.map { case (feature, label) => feature.max }.max() + 1
    println(s"feat length: $featLength")

    val instances = tempInstances.map { case (feat, label) => (new OneHotVector(featLength, feat), label)}
    instances.cache()
    println(s"instance num: ${instances.count()}")

    updateType match {
      case "spark" =>
        println(s"run spark sgd")
        runSGD(instances, featLength.toInt, stepSize, maxIter)
      case "ps" =>
        println(s"run ps sgd")
        runPSSGD(instances, featLength.toInt, stepSize, maxIter)
      case _ => println(s"wrong update type: $updateType (spark or ps)")
    }
  }


  def runSGD(trainData: RDD[(OneHotVector, Double)], dim: Int, stepSize: Double, maxIter: Int): Unit = {
    val initWeight = new DenseVector[Double](dim)
    val sgd = StochasticGradientDescent[DenseVector[Double]](stepSize, maxIter)
    val states = sgd.iterations(SparseLogistic.Cost(trainData), initWeight)

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
    println(s"weights: ${weight.toArray.take(100).mkString(" ")}")
  }

  def runPSSGD(trainData: RDD[(OneHotVector, Double)], dim: Int, stepSize: Double, maxIter: Int): Unit = {
    val initWeightPS = PSVector.dense(dim).toBreeze
    val sgd = StochasticGradientDescent[BreezePSVector](stepSize, maxIter)
    val states = sgd.iterations(SparseLogistic.PSCost(trainData), initWeightPS)

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
    println(s"weights: ${weight.pull.toDense.values.take(100).mkString(" ")}")
  }

}
