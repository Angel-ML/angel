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

package com.tencent.angel.spark.examples

import breeze.linalg.DenseVector

import com.tencent.angel.spark.models.vector.enhanced.CachedPSVector
import org.apache.spark.rdd.RDD

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.examples.util.Logistic
import com.tencent.angel.spark.examples.util.SparkUtils._
import com.tencent.angel.spark.models.vector.{DensePSVector, PSVector}
import com.tencent.angel.spark.rdd.RDDPSFunctions._
import com.tencent.angel.spark.linalg.{DenseVector => SONADV}

/**
 * These are examples of RDDFunction.psAggregate and RDDFunction.foldLeft
 */
object VectorAggregation {

  def main(args: Array[String]): Unit = {
    parseArgs(args)
    runSpark(this.getClass.getSimpleName) { sc =>
      PSContext.getOrCreate(sc)
      val vectorRDD = Logistic.generateLRData(N, DIM, numSlices)
        .map (x => new DenseVector[Double](x._1.toArray))

      run(vectorRDD)
      runWithPS(vectorRDD, DIM)
    }
  }

  private def run(data: RDD[DenseVector[Double]]): Unit = {
    println("sum" + data.reduce(_ + _))
    println("max" + data.reduce(breeze.linalg.max(_, _)))
    println("min" + data.reduce(breeze.linalg.min(_, _)))
  }

  private def runWithPS(data: RDD[DenseVector[Double]], dim: Int): Unit = {

    var vecKey: DensePSVector = null
    var vec: CachedPSVector = null
    var result: CachedPSVector = null

    vecKey = PSVector.dense(dim)
    vec = vecKey.toCache
    result = data.psFoldLeft(vec) { (pv, bv) =>
      pv.incrementWithCache(new SONADV(bv.toArray))
      pv
    }
    println("sum" + result.pullFromCache().toDense.values.mkString(", "))

    vecKey.fill(Double.NegativeInfinity)
    vec = vecKey.toCache
    result = data.psFoldLeft(vec) { (pv, bv) =>
      pv.mergeMaxWithCache(bv.toArray)
      pv
    }
    println("max" + result.pullFromCache().toDense.values.mkString(", "))

    vecKey.fill(Double.PositiveInfinity)
    vec = vecKey.toCache
    result = data.psFoldLeft(vec) { (pv, bv) =>
      pv.mergeMinWithCache(bv.toArray)
      pv
    }
    println("min" + result.pullFromCache().toDense.values.mkString(", "))

  }

}
