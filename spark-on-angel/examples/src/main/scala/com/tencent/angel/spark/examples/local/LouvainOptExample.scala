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

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.algo.communitydetection.louvainRepair.LouvainOpt
import com.tencent.angel.spark.ml.graph.algo.communitydetection.louvain.Louvain
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object LouvainOptExample {

  def main(args: Array[String]): Unit = {
    val mode = "local"
    val input = "data/graph/karate_club_network.txt"
    val output = "model/louvain_opt"
    val partitionNum = 1
    val storageLevel = StorageLevel.MEMORY_ONLY
    val numFold = 5
    val numOpt = 5
    val batchSize = 2
    val enableCheck = true
    val eps = 0.0001
    val bufferSize = 1000
    val isWeighted = false
    val psPartitionNum = 2
    val preserveRate = 0
    val useMergeStrategy = true

    val useBalancePartition = true
    val balancePartitionPercent = 0.7f


    start(mode)
    val louvain = new LouvainOpt()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setNumFold(numFold)
      .setNumOpt(numOpt)
      .setBatchSize(batchSize)
      .setDebugMode(enableCheck)
      .setEps(eps)
      .setBufferSize(bufferSize)
      .setIsWeighted(isWeighted)
      .setPSPartitionNum(psPartitionNum)
      .setPreserveRate(preserveRate)
      .setUseMergeStrategy(useMergeStrategy)
      .setUseBalancePartition(useBalancePartition)
      .setBalancePartitionPercent(balancePartitionPercent)


    val df = GraphIO.load(input, isWeighted = isWeighted)
    val mapping = louvain.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("louvainOpt")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    //conf.set("spark.driver.bindAddress", "localhost")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("cp")
    sc.setLogLevel("WARN")
    PSContext.getOrCreate(sc)
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
