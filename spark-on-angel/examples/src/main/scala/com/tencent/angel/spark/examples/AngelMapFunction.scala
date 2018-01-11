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
import breeze.math.MutableEnumeratedCoordinateField

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.examples.psf._
import com.tencent.angel.spark.examples.util.SparkUtils._
import com.tencent.angel.spark.models.vector.PSVector

/**
 * These are the examples of PS Oriented Functions(POF) in machine learning cases.
 */
object AngelMapFunction {

  def main(args: Array[String]): Unit = {
    parseArgs(args)
    runSpark(this.getClass.getSimpleName) { sc =>
      PSContext.getOrCreate(sc)
      run()
      runWithPSBreeze()
    }
  }

  def run()(implicit space: MutableEnumeratedCoordinateField[DenseVector[Double], Int, Double])
  : Unit = {
    val a = DenseVector.fill(DIM, 1.0)
    val b = DenseVector.fill(DIM, 2.0)
    val c = a.map(_ * 4.2)
    val d = space.zipMapValues.map(a, b, (x, y) => x / y)
    val e = a.mapPairs((index, value) => if (index == 2) 0 else value)
    val f = space.zipMapKeyValues.map(a, b, (index, x, y) => if (index == 2) 0 else x + y)
    println("c: " + c)
    println("d: " + d)
    println("e: " + e)
    println("f: " + f)
  }

  def runWithPSBreeze(): Unit = {
    val poolCapacity = 20

    val a = PSVector.dense(DIM).fill(1.0)
    val b = PSVector.duplicate(a).fill(2.0).toBreeze

    val aa = a.toBreeze

    val c = aa.map(new MulScalar(4.2))
    val d = aa.zipMap(b, new ZipDiv)
    val e = aa.mapWithIndex(new Filter(2))
    val f = aa.zipMapWithIndex(b, new FilterZipAdd(2))

    println("c: " + c.pull.toDense.values.mkString("Array(", ", ", ")"))
    println("d: " + d.pull.toDense.values.mkString("Array(", ", ", ")"))
    println("e: " + e.pull.toDense.values.mkString("Array(", ", ", ")"))
    println("f: " + f.pull.toDense.values.mkString("Array(", ", ", ")"))

    val g = b.zipMap(c, d, new Zip3Add)
    println("g: " + g.pull.toDense.values.mkString("Array(", ", ", ")"))

  }

}
