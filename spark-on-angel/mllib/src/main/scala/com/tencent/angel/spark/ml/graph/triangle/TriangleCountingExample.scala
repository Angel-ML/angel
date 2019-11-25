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
package com.tencent.angel.spark.ml.graph.triangle

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.utils.{Delimiter, GraphIO}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object TriangleCountingExample {

  def main(args: Array[String]): Unit = {
    val mode = "local"
    val input = "data/bc/edge"
    val sep = Delimiter.parse(Delimiter.SPACE)
    val output = "model/triangle_counting"
    val partitionNum = 4
    val storageLevel = StorageLevel.MEMORY_ONLY
    val batchSize = 1000
    val pullBatchSize = 1000
    val enableCheck = true
    val bufferSize = 100000
    val psPartitionNum = 2
    val srcIndex = 0
    val dstIndex = 1

    start(mode)
    val startTime = System.currentTimeMillis()
    val tc = new TriangleCountingUndirected()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setBatchSize(batchSize)
      .setPullBatchSize(pullBatchSize)
      .setDebugMode(enableCheck)
      .setBufferSize(bufferSize)
      .setPSPartitionNum(psPartitionNum)
      .setInput(input)
      .setSrcNodeIndex(srcIndex)
      .setDstNodeIndex(dstIndex)

    val df = GraphIO.load(input, isWeighted = false, sep = sep)
    val mapping = tc.transform(df)
    GraphIO.save(mapping, output)

    println(s"${System.currentTimeMillis() - startTime} ms elapsed")
    stop()
  }

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("triangle_counting")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("cp")
    sc.setLogLevel("WARN")
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
