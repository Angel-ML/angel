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

package com.tencent.angel.spark.examples.local

import com.tencent.angel.graph.rank.swing.Swing
import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SwingExample {

  def main(args: Array[String]): Unit = {
    val mode = "local"
    val input = "data/bc/edge"
    val output = "model/swing"
    val partitionNum = 1
    val storageLevel = StorageLevel.MEMORY_ONLY
    val psPartitionNum = 1
    val batchSize = 1000
    val pullBatchSize = 100
    val alpha = 0
    val topFrom = 1000
    val topTo = 1005
    val beta = 5
    val gamma = -0.3f
    val superItemThreshold = 20
    val superItemPairBatch = 100

    start(mode)
    val startTime = System.currentTimeMillis()
    val swing = new Swing()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setBatchSize(batchSize)
      .setPullBatchSize(pullBatchSize)
      .setPSPartitionNum(psPartitionNum)
      .setDelta(alpha)
      .setBeta(beta)
      .setGamma(gamma)
      .setSuperItemThreshold(superItemThreshold)
    swing.setItemFrom(topFrom)
    swing.setItemTo(topTo)
    swing.setSuperItemPairBatch(superItemPairBatch)

    val df = GraphIO.load(input, isWeighted = false, sep = " ")

    val mapping = swing.transform(df)
    GraphIO.save(mapping, output)

    println(s"cost ${System.currentTimeMillis() - startTime} ms")
    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()

    conf.setMaster(mode)
    conf.setAppName("swing")
    val sc = SparkContext.getOrCreate(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
