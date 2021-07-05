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

import com.tencent.angel.graph.community.copra.COPRA
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import com.tencent.angel.graph.utils.GraphIO

// support balance partition and weighted graph
object CopraExample {

  def main(args: Array[String]): Unit = {

    val mode = "local"
    val input = "data/bc/karate_club_network.txt"
    val output = "/Users/jiangyasong/Documents/dataSet/output"
    val partitionNum = 1
    val storageLevel = StorageLevel.MEMORY_ONLY
    val batchSize = 2
    val isWeighted = false
    val psPartitionNum = 2
    val src = 0
    val dst = 1
    val weightIndex = 2
    val useBalancePartition = true
    val balancePartitionPercent = 0.7f
    val useEdgeBalancePartition = false

    val arrayBoundsPath = null
    val numMaxCommunities = 5
    val preserveRate = 0.1f
    val maxIteration = 3

    val sep = " "

    val sc = start(mode)
    val copra = new COPRA()
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")
      .setNumMaxCommunities(numMaxCommunities)
      .setBatchSize(batchSize)
      .setMaxIteration(maxIteration)
      .setPreserveRate(preserveRate)
      .setArrayBoundsPath(arrayBoundsPath)
      .setPartitionNum(partitionNum)
      .setIsWeighted(isWeighted)
      .setUseEdgeBalancePartition(useEdgeBalancePartition)
      .setBalancePartitionPercent(balancePartitionPercent)
      .setUseBalancePartition(useBalancePartition)

    val df = GraphIO.load(input, isWeighted = isWeighted, src, dst, weightIndex, sep = sep)

    val mapping = copra.transform(df)

    GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("copra")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //PSContext.getOrCreate(sc)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
