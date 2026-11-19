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

package com.tencent.angel.spark.examples.basic

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.models.PSVector
import com.tencent.angel.spark.util.VectorUtils
import org.apache.spark.{SparkConf, SparkContext}

object PSVectorExample {

  def start(args: Array[String] = Array.empty[String]): Unit = {
    val params = com.tencent.angel.spark.ml.core.ArgsUtil.parse(args)
    val conf = new SparkConf()
    conf.setMaster(params.getOrElse("mode", conf.get("spark.master", "local[1]")))
    conf.setAppName("PSVector Examples")
    conf.setIfMissing("spark.ps.instances", "1")
    conf.setIfMissing("spark.ps.cores", "1")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

  def main(args: Array[String]): Unit = {

    start(args)

    // create a dense PSVector with 10 dimension (also create a 5x10 dense matrix)
    val dense = PSVector.dense(10, 5)
    // create a sparse PSVector with 10 dimension (also create a 5x10 sparse matrix)
    val sparse = PSVector.sparse(10, 5)

    // initialize
    dense.fill(1.0)

    // duplicate two vector from the same matrix
    val x = PSVector.duplicate(dense)
    val y = PSVector.duplicate(dense)
    x.fill(1.0)
    y.fill(2.0)

    // dot-product operation
    val dot = VectorUtils.dot(x, y)
    require(math.abs(dot - 20.0) < 1e-8, "Unexpected PS vector dot product")
    println(dot)

    // pull the whole vector to local
    val vector = dense.pull()
    require(math.abs(vector.sum() - 10.0) < 1e-8, "PS vector fill failed")
    println(vector.sum())

    // pull the vector with indices
    val partial = dense.pull(Array(1, 2, 3, 4))
    println(partial.sum())

    // increment the vector
    val delta = VFactory.sparseDoubleVector(10, Array(1, 2, 3, 4), Array(1.0, 1.0, 1.0, 1.0))
    dense.increment(delta)

    // update the vector
    val update = VFactory.sparseDoubleVector(10, Array(1, 2), Array(0.0, 0.0))
    dense.update(update)

    val updatedSum = dense.pull(Array(1, 2, 3)).sum()
    require(math.abs(updatedSum - 2.0) < 1e-8, "PS vector update failed")
    println(updatedSum)


    stop()

  }

}
