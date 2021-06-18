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

import com.tencent.angel.graph.connectedcomponent.wcc.WCC
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.graph.utils.{Delimiter, GraphIO}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object CCExample {
  def main(args: Array[String]): Unit = {
    
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val sc = start(mode)
    
    val input = params.getOrElse("input", null)
    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val compressIterNum = params.getOrElse("compressIterNum", "3").toInt
    val localLimit = params.getOrElse("localLimit", "100000000").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val output = params.getOrElse("output", null)
    val srcIndex = params.getOrElse("src", "0").toInt
    val dstIndex = params.getOrElse("dst", "1").toInt
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val pullBatchSize = params.getOrElse("pullBatchSize", "2000").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum",
      sc.getConf.get("spark.ps.instances", "10")).toInt
    val useBalancePartition = params.getOrElse("useBalancePartition", "false").toBoolean
    val balancePartitionPercent = params.getOrElse("balancePartitionPercent", "0.7").toFloat
    
    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    sc.setCheckpointDir(cpDir)
  
    val sep = Delimiter.parse(params.getOrElse("sep", Delimiter.TAB))
    
    val ccc = new WCC()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setCompressIterNum(compressIterNum)
      .setLocalLimit(localLimit)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")
      .setUseBalancePartition(useBalancePartition)
      .setBalancePartitionPercent(balancePartitionPercent)
      .setBatchSize(batchSize)
      .setPullBatchSize(pullBatchSize)
    
    val df = GraphIO.load(input, isWeighted = false, srcIndex, dstIndex, sep = sep)
    
    val mapping = ccc.transform(df)
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
    conf.setAppName("CompressCC")
    val sc = new SparkContext(conf)
    //PSContext.getOrCreate(sc)
    sc
  }
  
  def stop(): Unit = {
    //PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
