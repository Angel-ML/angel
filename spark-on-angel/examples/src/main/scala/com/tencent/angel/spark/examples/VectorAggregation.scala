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
import org.apache.spark.rdd.RDD

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.examples.util.Logistic
import com.tencent.angel.spark.examples.util.PSExamples._
import com.tencent.angel.spark.model.PSModelProxy
import com.tencent.angel.spark.model.vector.RemotePSVector
import com.tencent.angel.spark.rdd.RDDPSFunctions._

/**
 * These are examples of RDDFunction.psAggregate and RDDFunction.foldLeft
 */
object VectorAggregation {

  def main(args: Array[String]): Unit = {
    parseArgs(args)
    runWithSparkContext(this.getClass.getSimpleName) { sc =>
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

    val psContext = PSContext.getOrCreate()
    val pool = psContext.createModelPool(dim, 2)

    var vecKey: PSModelProxy = null
    var vec: RemotePSVector = null
    var result: RemotePSVector = null

    vecKey = pool.createZero()
    vec = vecKey.mkRemote()
    result = data.psFoldLeft(vec) { (pv, bv) =>
      pv.increment(bv.toArray)
      pv
    }
    println("sum" + result.pull().mkString(", "))
    vecKey.delete()

    vecKey = pool.createModel(Double.NegativeInfinity)
    vec = vecKey.mkRemote()
    result = data.psFoldLeft(vec) { (pv, bv) =>
      pv.mergeMax(bv.toArray)
      pv
    }
    println("max" + result.pull().mkString(", "))
    vecKey.delete()

    vecKey = pool.createModel(Double.PositiveInfinity)
    vec = vecKey.mkRemote()
    result = data.psFoldLeft(vec) { (pv, bv) =>
      pv.mergeMin(bv.toArray)
      pv
    }
    println("min" + result.pull().mkString(", "))
    vecKey.delete()

    psContext.destroyModelPool(pool)
  }

}
