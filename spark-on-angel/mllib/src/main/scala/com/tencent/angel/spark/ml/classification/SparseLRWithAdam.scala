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

package com.tencent.angel.spark.ml.classification

import breeze.linalg.DenseVector
import breeze.optimize.StochasticGradientDescent
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.vector.PSVector
import com.tencent.angel.spark.models.vector.enhanced.BreezePSVector
import com.tencent.angel.spark.ml.sparse.SparseLogistic
import com.tencent.angel.spark.ml.util.{ArgsUtil, DataLoader}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import com.tencent.angel.spark.linalg.OneHotVector
import com.tencent.angel.spark.ml.optimize.Adam

/**
  * Train Logistic Regression with Adam SGD.
  *
  */
object SparseLRWithAdam {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params("input")
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val stepSize = params.getOrElse("stepSize", "0.01").toDouble
    val maxIter = params.getOrElse("maxIter", "50").toInt
    val updateType = params.getOrElse("updateType", "spark")  // spark or ps

    val l2Reg = params.getOrElse("l2Reg", "0.01").toDouble
    val rho1 = params.getOrElse("rho1", "0.9").toDouble
    val rho2 = params.getOrElse("rho2", "0.999").toDouble
    val epsilon = params.getOrElse("epsilon", "1e-8").toDouble

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master(mode)
      .getOrCreate()

    if (updateType.startsWith("ps")) PSContext.getOrCreate(spark.sparkContext)

    val tempInstances = DataLoader.loadOneHotInstance(input, partitionNum, sampleRate)
      .rdd.map { row =>
        Tuple2(row.getAs[scala.collection.mutable.WrappedArray[Long]](1).toArray, row.getString(0).toDouble)
      }
    val featDim = tempInstances.map {
      case (feature, label) => feature.max
      }.max() + 1
    println(s"feat num: $featDim")

    val instances = tempInstances.map { case (feat, label) => (new OneHotVector(featDim, feat), label)}
    instances.cache()
    println(s"instance num: ${instances.count()}")

    updateType match {
      case "spark" =>
        println(s"run spark Adam SGD")
        runSGD(instances, featDim.toInt,
          stepSize, maxIter, l2Reg, rho1, rho2, epsilon)
      case "ps-dense" =>
        println(s"run ps dense Adam SGD")
        runPSDenseAdam(instances, featDim.toInt,
          stepSize, maxIter, l2Reg, rho1, rho2, epsilon)
      case "ps-sparse" =>
        println(s"run ps sparse Adam SGD")
        runPSSparseAdam(instances, featDim.toInt,
          stepSize, maxIter, l2Reg, rho1, rho2, epsilon)
      case _ => println(s"wrong update type: $updateType (spark or ps)")
    }
  }

  def runSGD(trainData: RDD[(OneHotVector, Double)],
              dim: Int,
              stepSize: Double,
              maxIter: Int,
              l2Reg: Double,
              rho1: Double,
              rho2: Double,
              epsilon: Double
             ): Unit = {
    val initWeight = new DenseVector[Double](dim)
    val optimizer = StochasticGradientDescent[DenseVector[Double]](stepSize, maxIter)
    val states = optimizer.iterations(SparseLogistic.Cost(trainData), initWeight)

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

  def runPSDenseAdam(trainData: RDD[(OneHotVector, Double)],
                dim: Int,
                stepSize: Double,
                maxIter: Int,
                l2Reg: Double,
                rho1: Double,
                rho2: Double,
                epsilon: Double
             ): Unit = {
    val initWeightPS = PSVector.dense(dim).toBreeze
    val optimizer = new Adam(
      stepSize, maxIter, l2Reg, rho1, rho2, epsilon)
    val states = optimizer.iterations(SparseLogistic.PSCost(trainData), initWeightPS)

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
    println(s"weight ${weight.pull.toDense.values.slice(0, 10).mkString(" ")}")
  }

  def runPSSparseAdam(trainData: RDD[(OneHotVector, Double)],
                     dim: Int,
                     stepSize: Double,
                     maxIter: Int,
                     l2Reg: Double,
                     rho1: Double,
                     rho2: Double,
                     epsilon: Double
                    ): Unit = {
    val initWeightPS = PSVector.sparse(dim).toBreeze
    val optimizer = new Adam(
      stepSize, maxIter, l2Reg, rho1, rho2, epsilon)
    val states = optimizer.iterations(SparseLogistic.PSCost(trainData), initWeightPS)

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
    val localWeight = weight.pull.toSparse
    println(s"weights: ${localWeight.nnz} " +
      s"index: ${localWeight.indices.take(10).mkString(" ")} " +
      s"value: ${localWeight.values.take(10).mkString(" ")}")
  }


}
