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

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ps.storage.matrix.PartitionSourceArray
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.graph.statistics.commonfriends._
import com.tencent.angel.graph.utils.{Delimiter, GraphIO}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object CommonFriendsExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val sc = start(mode)

    val input = params.getOrElse("input", null)
    val extraInput = params.getOrElse("extraInput", null)
    val sep = Delimiter.parse(params.getOrElse("sep", Delimiter.SPACE))
    val output = params.getOrElse("output", null)

    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum",
      sc.getConf.get("spark.ps.instances", "10")).toInt

    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val pullBatchSize = params.getOrElse("pullBatchSize", "1000").toInt

    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val enableCheck = params.getOrElse("enableCheck", "false").toBoolean
    val bufferSize = params.getOrElse("bufferSize", "1000000").toInt

    val isCompressed = params.getOrElse("isCompressed", "false").toBoolean
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val compressIndex = params.getOrElse("compressIndex", "2").toInt

    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    sc.setCheckpointDir(cpDir)

    start(mode)
    val startTime = System.currentTimeMillis()
    val commonFriends = new CommonFriends()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setBatchSize(batchSize)
      .setPullBatchSize(pullBatchSize)
      .setDebugMode(enableCheck)
      .setBufferSize(bufferSize)
      .setIsCompressed(isCompressed)
      .setPSPartitionNum(psPartitionNum)
      .setInput(input)
      .setExtraInputs(Array(extraInput))
      .setSrcNodeIndex(srcIndex)
      .setDstNodeIndex(dstIndex)
      .setCompressIndex(compressIndex)
      .setDelimiter(sep)

    val df = GraphIO.load(input, isWeighted = isCompressed,
      srcIndex = srcIndex, dstIndex = dstIndex, weightIndex = compressIndex, sep = sep)
    val mapping = commonFriends.transform(df)
    GraphIO.save(mapping, output)

    println(s"cost ${System.currentTimeMillis() - startTime} ms")
    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()

    // Set specific parameters for LINE
    conf.set(AngelConf.ANGEL_PS_PARTITION_SOURCE_CLASS, classOf[PartitionSourceArray].getName)
    // Close the automatic checkpoint
    conf.set("spark.hadoop." + AngelConf.ANGEL_PS_BACKUP_AUTO_ENABLE, "false")
    conf.set("spark.hadoop." + AngelConf.ANGEL_PS_JVM_USE_PARALLEL_GC, "true")
    conf.set("spark.hadoop." + AngelConf.ANGEL_PS_JVM_PARALLEL_GC_USE_ADAPTIVE_SIZE, "false")
    conf.set("io.file.buffer.size", "16000000");
    conf.set("spark.hadoop.io.file.buffer.size", "16000000");

    // Add jvm parameters for executors
    val executorJvmOptions = "-verbose:gc -XX:-PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:<LOG_DIR>/gc.log " +
      "-XX:+UseG1GC -XX:MaxGCPauseMillis=1000 -XX:G1HeapRegionSize=32M " +
      "-XX:InitiatingHeapOccupancyPercent=60 -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 "

    println(s"executorJvmOptions = ${executorJvmOptions}")
    conf.set("spark.executor.extraJavaOptions", executorJvmOptions)

    conf.setMaster(mode)
    conf.setAppName("commonfriends")
    val sc = SparkContext.getOrCreate(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}
