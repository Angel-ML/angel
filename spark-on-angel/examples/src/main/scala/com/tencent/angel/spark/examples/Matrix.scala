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

package com.tencent.angel.spark.examples

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.examples.util.SparkUtils._
import com.tencent.angel.spark.models.matrix.DensePSMatrix
import scala.util.Random

object Matrix {
  def main(args: Array[String]): Unit = {
    runSpark(this.getClass.getSimpleName) { sc =>
      PSContext.getOrCreate(sc)
      run(args)
    }
  }

  def run(args: Array[String]): Unit = {
    val dim = args(0).toInt

    val rand = new Random()
    val diagArray = (0 until dim).toArray.map (i => rand.nextDouble())
    println(s"diag array: ${diagArray.slice(0, 10).mkString(" ")}")

    val mat = DensePSMatrix.diag(diagArray)

    val localMat = mat.pull()
    val localDiag = (0 until dim).toArray.map(id => localMat(id)(id))
    println(s"matrix diag: ${localDiag.slice(0, 10).mkString(" ")}")

    var missMatchCount = 0
    (0 until dim).foreach { id =>
      val tol = math.abs(diagArray(id) - localDiag(id))
      if (tol > 1e-8) {
        missMatchCount += 1
        println(s"dimension id: $id not match")
      }
    }
    println(s"miss match count: $missMatchCount")
  }
}
