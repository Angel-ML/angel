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

package com.tencent.angel.spark.examples.ml

import breeze.linalg.DenseVector
import org.apache.spark.sql.SparkSession

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.examples.util.Logistic
import com.tencent.angel.spark.models.vector.PSVector


object AngelLR {
  import com.tencent.angel.spark.examples.util.PSExamples._

  def main(args: Array[String]): Unit = {
    parseArgs(args)
    runSpark(this.getClass.getSimpleName) { sc =>
      PSContext.getOrCreate(sc)
      execute(DIM, N, numSlices, ITERATIONS)
    }
  }

  def execute(
      dim: Int,
      sampleNum: Int,
      partitionNum: Int,
      maxIter: Int,
      stepSize: Double = 0.1): Unit = {

    val trainData = Logistic.generateLRData(sampleNum, dim, partitionNum)

    val w = PSVector.dense(dim)
    val sc = SparkSession.builder().getOrCreate().sparkContext

    for (i <- 1 to ITERATIONS) {
      val bcW = sc.broadcast(w.pull())
      val totalG = PSVector.duplicate(w)

      val tempRDD = trainData.mapPartitions { iter =>
        val breezeW = new DenseVector(bcW.value)

        val subG = iter.map { case (feat, label) =>
          val brzData = new DenseVector[Double](feat.toArray)
          val margin: Double = -1.0 * breezeW.dot(brzData)
          val gradientMultiplier = (1.0 / (1.0 + math.exp(margin))) - label
          val gradient = brzData * gradientMultiplier
          gradient
        }.reduce(_ + _)
        totalG.increment(subG.toArray)
        Iterator.empty
      }
      tempRDD.count()
      w.toBreeze -= (totalG.toBreeze :* (1.0 / sampleNum))
    }

    println(s"w: ${w.pull().mkString(" ")}")
  }

}
