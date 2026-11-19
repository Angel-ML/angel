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
import com.tencent.angel.spark.ml.graph.algo.connectedcomponent.wcc.BatchedCC
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.DataStatistics
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object BatchedCCExample {
  def main(args: Array[String]): Unit = {
    
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn")
    val sc = start(mode)
    
    val input = params.getOrElse("input", null)
    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val batchNum = params.getOrElse("batchNum", "4").toInt
    val output = params.getOrElse("output", null)
    val srcIndex = params.getOrElse("src", "0").toInt
    val dstIndex = params.getOrElse("dst", "1").toInt

    val psPartitionNum = params.getOrElse("psPartitionNum",
      sc.getConf.get("spark.ps.instances", "10")).toInt

    val useBalancePartition = params.getOrElse("useBalancePartition", "false").toBoolean
    val balancePartitionPercent = params.getOrElse("balancePartitionPercent", "0.7").toFloat
    
    val needReplicaEdge = params.getOrElse("needReplicaEdge", "true").toBoolean
    
    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    sc.setCheckpointDir(cpDir)
    
    val sep = params.getOrElse("sep",  "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }
    
    val bcc = new BatchedCC()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setbatchNum(batchNum)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")
      .setUseBalancePartition(useBalancePartition)
      .setBalancePartitionPercent(balancePartitionPercent)
      .setNeedReplicaEdge(needReplicaEdge)
    
    val df = GraphIO.load(input, isWeighted = false, srcIndex, dstIndex, sep = sep)

    //collect app name and data size
    DataStatistics.setAppName(bcc.getClass.getCanonicalName)
    DataStatistics.calcRDDSize(df, "angel.input.data")
    DataStatistics.setDataPartitionNum(partitionNum)
    DataStatistics.setPSPartitionNum(psPartitionNum)

    val mapping = bcc.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }
  
  def start(mode: String): SparkContext = {
    val conf = new SparkConf()
    
    // Add jvm parameters for executors
    if (mode != "local") {
      var executorJvmOptions = conf.get("spark.executor.extraJavaOptions")
      executorJvmOptions += " -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 -Xss4M "
      conf.set("spark.executor.extraJavaOptions", executorJvmOptions)
      println(s"executorJvmOptions = ${executorJvmOptions}")
    }
    
    
    conf.setMaster(mode)
    conf.setAppName("batchedcc")
    val sc = new SparkContext(conf)
    sc
  }
  
  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
