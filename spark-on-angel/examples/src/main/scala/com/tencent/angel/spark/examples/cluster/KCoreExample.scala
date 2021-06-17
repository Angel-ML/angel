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
import com.tencent.angel.graph.rank.kcore.KCore
import com.tencent.angel.graph.utils.{Delimiter, GraphIO}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object KCoreExample {
  def main(args: Array[String]): Unit = {
    val t1 = System.currentTimeMillis()
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "local")
    val sc = start(mode)
    
    val input = params.getOrElse("input", null)
    val partitionNum = params.getOrElse("partitionNum", "1").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val pullBatchSize = params.getOrElse("pullBatchSize", "1000").toInt
    val output = params.getOrElse("output", null)
    val srcIndex = params.getOrElse("src", "0").toInt
    val dstIndex = params.getOrElse("dst", "1").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum",
      sc.getConf.get("spark.ps.instances", "2")).toInt
    val useBalancePartition = params.getOrElse("useBalancePartition", "false").toBoolean
    val balancePartitionPercent = params.getOrElse("balancePartitionPercent", "0.7").toFloat
    var degreeThreshold = params.getOrElse("degreeThreshold", "1").toInt
    val initialCorePath = params.getOrElse("initialCorePath", null)
    val needReplicaEdge = params.getOrElse("needPeplicaEdge", "false").toBoolean
    val execMode = params.getOrElse("execMode", "full")
    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    val numFirsts = params.getOrElse("numFirsts", "1").toInt
    val staticCorePath = params.getOrElse("staticCorePath", null)
    val checkpoint = params.getOrElse("checkpoint", "false").toBoolean
    
    sc.setCheckpointDir(cpDir)
  
    val sep = Delimiter.parse(params.getOrElse("sep", Delimiter.TAB))
    
    if (execMode.toLowerCase == "full" || execMode.toLowerCase == "sparse") {
      degreeThreshold = -1
    }
    
    val kCore = new KCore()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")
      .setUseBalancePartition(useBalancePartition)
      .setBalancePartitionPercent(balancePartitionPercent)
      .setBatchSize(batchSize)
      .setPullBatchSize(pullBatchSize)
      .setTopK(degreeThreshold)
      .setBufferSize(numFirsts)
      .setCheckPoint(checkpoint)
      .setExecMode(execMode)
      .setNeedReplicaEdge(needReplicaEdge)
    
    if (initialCorePath != null) {
      val initialCore = GraphIO.load(initialCorePath, false, sep = "\t")
        .select("src", "dst").rdd
        .map(x => (x.getLong(0), x.getLong(1).toInt))
      kCore.setInitialCore(initialCore)
    }
    
    if (execMode.toLowerCase() == "sparse" || execMode.toLowerCase() == "mid") {
      assert(staticCorePath != null, s"error: staticCorePath is null")
      val t2 = System.currentTimeMillis()
      val staticCores = staticCorePath.split(",").map { path =>
        val staticCore = GraphIO.load(path, false, sep = "\t")
          .select("src")
        println(s"static cores from $path load")
        staticCore
      }.reduce((df1, df2) => df1.union(df2)).rdd
        .map(row => row.getLong(0))
        .persist(storageLevel)
      staticCores.count()
      println(s"read ${staticCores.count()} nodes with static cores. " +
        s"cost ${System.currentTimeMillis() - t2} ms")
      
      kCore.setStaticCores(staticCores)
    }
    
    val df = GraphIO.load(input, isWeighted = false, srcIndex, dstIndex, sep = sep)
    println(s"init cost: ${System.currentTimeMillis() - t1} ms")
    
    val mapping = kCore.transform(df)
    println(s"process cost: ${System.currentTimeMillis() - t1} ms")
    
    GraphIO.save(mapping, output)
    stop()
    println(s"total cost: ${System.currentTimeMillis() - t1} ms")
  }
  def start(mode: String): SparkContext = {
    val conf = new SparkConf()

    // Add jvm parameters for executors
    if (!mode.contains("local")) {
      var executorJvmOptions = conf.get("spark.executor.extraJavaOptions")
      executorJvmOptions += " -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 -Xss4M "
      conf.set("spark.executor.extraJavaOptions", executorJvmOptions)
      println(s"executorJvmOptions = ${executorJvmOptions}")
    }
    conf.setMaster(mode)
    conf.setAppName("K-Core")
    val sc = new SparkContext(conf)
    //PSContext.getOrCreate(sc)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
