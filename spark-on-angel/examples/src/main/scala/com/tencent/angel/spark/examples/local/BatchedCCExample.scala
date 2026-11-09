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
import com.tencent.angel.spark.ml.graph.algo.connectedcomponent.wcc.BatchedCC
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object BatchedCCExample {
  
  def main(args: Array[String]): Unit = {
    val t1 = System.currentTimeMillis()
    val mode = "local"
    //    val input = "E:\\GraphDataSet\\data\\web-NotreDame\\web-NotreDame.txt"
    //    val output = "E:\\GraphDataSet\\data\\web-NotreDame\\resDir"
    val input = "data/bc/sccEdge"
    val output = "model/bcc/"
    
    val partitionNum = 2
    val storageLevel = StorageLevel.MEMORY_ONLY
    val batchNum = 3
    val psPartitionNum = 2
    
    start(mode)
    val bcc = new BatchedCC()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setbatchNum(batchNum)
    
    val df = GraphIO.load(input, isWeighted = false)
    val mapping = bcc.transform(df)
    println("process cost is " + (System.currentTimeMillis() - t1) * 1.0f / 1000)
    GraphIO.save(mapping, output)
    println("cost time is " + (System.currentTimeMillis() - t1) * 1.0f / 1000)
    
    stop()
  }
  
  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("bcc")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    sc.setCheckpointDir("cp")
    PSContext.getOrCreate(sc)
  }
  
  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
  
}
