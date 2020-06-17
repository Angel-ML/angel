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

package com.tencent.angel.spark.examples.cluster

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.graph.statistics.triangle.TriangleCountingUndirected
import com.tencent.angel.graph.utils.{Delimiter, GraphIO}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object TriangleCountUndirectedExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val sc = start(mode)

    val input = params.getOrElse("input", null)
    val sep = Delimiter.parse(params.getOrElse("sep", Delimiter.TAB))
    val output = params.getOrElse("output", null)
    val partitionNum = params.getOrElse("partitionNum", "1000").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum",
      sc.getConf.get("spark.ps.instances", "100")).toInt
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val pullBatchSize = params.getOrElse("pullBatchSize", "1000").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val computeLCC = params.getOrElse("computeLCC", "false").toBoolean

    start(mode)
    val startTime = System.currentTimeMillis()
    val triangleCount = new TriangleCountingUndirected()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setBatchSize(batchSize)
      .setPullBatchSize(pullBatchSize)
      .setPSPartitionNum(psPartitionNum)
      .setComputeLcc(computeLCC)


    val df = GraphIO.load(input, isWeighted = false, sep = sep)
    val mapping = triangleCount.transform(df)
    GraphIO.save(mapping, output)

    println(s"cost ${System.currentTimeMillis() - startTime} ms")
    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("triangleCounting_undirected")
    val sc = SparkContext.getOrCreate(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}
