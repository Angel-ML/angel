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
import com.tencent.angel.spark.models.PSMatrix
import org.apache.spark.{SparkConf, SparkContext}

object PSMatrixExample {

  def start(): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[1]")
    conf.setAppName("PSVector Examples")
    conf.set("spark.ps.model", "LOCAL")
    conf.set("spark.ps.jars", "")
    conf.set("spark.ps.instances", "1")
    conf.set("spark.ps.cores", "1")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

  def main(args: Array[String]): Unit = {

    start()

    // randomly initialize a dense PSMatrix (5x10)
    val dense = PSMatrix.rand(5, 10)

    // pull the first row
    println(dense.pull(0).sum())
    // pull the first with indices (0, 1)
    println(dense.pull(0, Array(0, 1)).sum())

    // update the first row with indices(0, 1)
    val update = VFactory.sparseDoubleVector(10, Array(0, 1), Array(1.0, 1.0))
    dense.update(0, update)

    println(dense.pull(0, Array(0, 1)).sum())

    // reset the first row
    dense.reset(0)
    println(dense.pull(0).sum())

    stop()
  }

}
