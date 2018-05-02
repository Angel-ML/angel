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
import breeze.optimize.LBFGS
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.linalg.OneHotVector
import com.tencent.angel.spark.ml.sparse.SparseLogistic
import com.tencent.angel.spark.ml.util.{ArgsUtil, DataLoader}
import com.tencent.angel.spark.models.vector.PSVector
import com.tencent.angel.spark.models.vector.enhanced.BreezePSVector


object SparseLRWithLBFGS {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params("input")
    val partitionNum = params.getOrElse("partitionNum", "10").toInt
    val sampleRate = params.getOrElse("sampleRate", "1.0").toDouble
    val stepSize = params.getOrElse("m", "10").toInt
    val maxIter = params.getOrElse("maxIter", "50").toInt
    val updateType = params.getOrElse("updateType", "spark")  // spark or ps

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master(mode)
      .getOrCreate()

    if (updateType.startsWith("ps")) PSContext.getOrCreate(spark.sparkContext)

    val tempInstances = DataLoader.loadOneHotInstance(input, partitionNum, sampleRate).rdd
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
        println(s"run spark lbfgs")
        runLBFGSSpark(instances, featLength.toInt, stepSize, maxIter)
      case "ps-dense" =>
        println(s"run angel dense ps lbfgs")
        runDenseLBFGSAngel(instances, featLength.toInt, stepSize, maxIter)
      case "ps-sparse" =>
        println(s"run angel sparse ps lbfgs")
        runDenseLBFGSAngel(instances, featLength.toInt, stepSize, maxIter)
      case _ => println(s"wrong update type: $updateType (spark or ps-dense or ps-sparse)")
    }
  }


  def runLBFGSSpark(trainData: RDD[(OneHotVector, Double)], dim: Int, m: Int, maxIter: Int): Unit = {
    val initWeight = new DenseVector[Double](dim)
    val tol = 1e-9
    val lbfgs = new LBFGS[DenseVector[Double]](maxIter, m, tol)
    val states = lbfgs.iterations(SparseLogistic.Cost(trainData), initWeight)

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
    println(s"weights: ${weight.toArray.take(10).mkString(" ")}")
  }

  def runDenseLBFGSAngel(trainData: RDD[(OneHotVector, Double)], dim: Int, m: Int, maxIter: Int): Unit = {
    val initWeightPS = PSVector.dense(dim, 2 * m + 10).toBreeze
    val tol = 1e-6
    val lbfgs = new LBFGS[BreezePSVector](maxIter, m, tol)
    val states = lbfgs.iterations(SparseLogistic.PSCost(trainData), initWeightPS)

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

  def runSparseLBFGSAngel(trainData: RDD[(OneHotVector, Double)], dim: Int, m: Int, maxIter: Int): Unit = {
    val initWeightPS = PSVector.sparse(dim, 2 * m + 10).toBreeze
    val tol = 1e-6
    val lbfgs = new LBFGS[BreezePSVector](maxIter, m, tol)
    val states = lbfgs.iterations(SparseLogistic.SparsePSCost(trainData, 0.0), initWeightPS)

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
    println(s"weights nnz: ${localWeight.nnz} " +
      s"index: ${localWeight.indices.take(10).mkString(" ")} " +
      s"value: ${localWeight.values.take(10).mkString(" ")}")
  }

}
