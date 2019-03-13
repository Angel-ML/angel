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

package com.tencent.angel.spark.ml.kcore

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


object KCore {

  //  private val LOG = LoggerFactory.getLogger(this.getClass)

  def process(
      graph: RDD[KCoreGraphPartition],
      partitionNum: Int,
      maxIdOption: Option[Int],
      storageLevel: StorageLevel,
      switchRate: Double = 0.001): RDD[(Array[Int], Array[Int])] = {

    val maxId = maxIdOption.getOrElse {
      graph.map(_.max).aggregate(Int.MinValue)(math.max, math.max)
    }
    val model = KCorePSModel.fromMaxId(maxId + 1)

    // init
    graph.foreach(_.init(model))
    println(s"init core sum: ${graph.map(_.sum(model)).sum()}")


    var numMsg = Long.MaxValue
    var iterNum = 0
    var version = 0

    while (numMsg > 0) {
      iterNum += 1
      version += 1
      numMsg = graph.map(_.process(model, version, numMsg < maxId * switchRate)).reduce(_ + _)
      println(s"iter-$iterNum, num node updated: $numMsg")

      // reset version
      if (Coder.isMaxVersion(version + 1)) {
        println("reset version")
        version = 0
        graph.foreach(_.resetVersion(model))
      }

      // show sum of cores every 10 iter
      if (iterNum % 10 == 0) {
        val sum = graph.map(_.sum(model)).sum()
        println(s"iter-$iterNum, core sum = $sum")
      }
    }

    println(s"iteration end in $iterNum round, final core sum is ${graph.map(_.sum(model)).sum()}")

    // save
    graph.map(_.save(model))
  }
}
