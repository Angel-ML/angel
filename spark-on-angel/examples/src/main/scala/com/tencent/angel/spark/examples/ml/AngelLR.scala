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
import org.apache.spark.SparkContext

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.examples.util.Logistic
import com.tencent.angel.spark.models.vector.PSVector
import com.tencent.angel.spark.linalg.{DenseVector => SONADV}


object AngelLR {
  import com.tencent.angel.spark.examples.util.SparkUtils._

  def main(args: Array[String]): Unit = {
    parseArgs(args)
    runSpark(this.getClass.getSimpleName) { sc =>
      execute(sc, DIM, N, numSlices, ITERATIONS)
    }
  }

  def execute(
      sc: SparkContext,
      dim: Int,
      sampleNum: Int,
      partitionNum: Int,
      maxIter: Int,
      stepSize: Double = 0.1): Unit = {

    PSContext.getOrCreate(sc)
    val trainData = Logistic.generateLRData(sampleNum, dim, partitionNum)
      .map { case (feat, label) =>
        if (label == 0.0) (feat, -1.0) else (feat, 1.0)
      }

    val psW = PSVector.dense(dim)
    val psG = PSVector.duplicate(psW)

    println("Initial psW: " + psW.dimension)

    for (i <- 1 to ITERATIONS) {
      println("On iteration " + i)

      val localW = new DenseVector(psW.pull.values)

      val temp = trainData.map { case (feature, label) =>
        val x = new DenseVector(feature.toArray)
        val g = -label * (1 - 1.0 / (1.0 + math.exp(-label * localW.dot(x)))) * x
        psG.toCache.increment(new SONADV(g.toArray))
      }
      temp.count()

      psW.toBreeze -= (psG.toBreeze :* (1.0 / sampleNum))
      psG.zero()
    }

    println(s"Final psW: ${psW.pull.values.mkString(" ")}")
  }

}
