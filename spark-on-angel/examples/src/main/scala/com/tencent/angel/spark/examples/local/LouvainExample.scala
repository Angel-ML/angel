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
import com.tencent.angel.graph.community.louvain.Louvain
import com.tencent.angel.graph.utils.GraphIO
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object LouvainExample {

  def main(args: Array[String]): Unit = {
    val mode = "local"
    val input = "data/bc/edge"
    val output = "model/louvain"
    val partitionNum = 4
    val storageLevel = StorageLevel.MEMORY_ONLY
    val numFold = 2
    val numOpt = 5
    val batchSize = 100
    val enableCheck = true
    val eps = 0.0
    val bufferSize = 100000
    val isWeighted = false
    val psPartitionNum = 2

    start(mode)
    val louvain = new Louvain()
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
    val df = GraphIO.load(input, isWeighted = isWeighted)
    val mapping = louvain.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("louvain")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
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
