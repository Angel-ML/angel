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
import com.tencent.angel.graph.statistics.lccUndirected.{GroupLccUndirected, LccUndirected}
import com.tencent.angel.graph.utils.{Delimiter, GraphIO}
import com.tencent.angel.ps.storage.matrix.PartitionSourceArray
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object LccUndirectedExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val sc = start(mode)

    val input = params.getOrElse("input", null)
    val userGroupInput = params.getOrElse("userGroupInput", null)
    val sep = Delimiter.parse(params.getOrElse("sep", Delimiter.SPACE))
    val output = params.getOrElse("output", null)

    val partitionNum = params.getOrElse("partitionNum", "1").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum",
      sc.getConf.get("spark.ps.instances", "1")).toInt
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val pullBatchSize = params.getOrElse("pullBatchSize", "1000").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val groupSrcIndex = params.getOrElse("groupSrcIndex", "0").toInt
    val groupDstIndex = params.getOrElse("groupDstIndex", "1").toInt

    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    sc.setCheckpointDir(cpDir)

    start(mode)
    val startTime = System.currentTimeMillis()
    val df = GraphIO.load(input, isWeighted = false, srcIndex = srcIndex, dstIndex = dstIndex, sep = sep)
//    DataStatistics.calcRDDSize(df, "angel.input.data")

    if (userGroupInput == null) {
      val lcc = new LccUndirected()
        .setPartitionNum(partitionNum)
        .setStorageLevel(storageLevel)
        .setBatchSize(batchSize)
        .setPullBatchSize(pullBatchSize)
        .setPSPartitionNum(psPartitionNum)
        .setInput(input)
        .setSrcNodeIndex(srcIndex)
        .setDstNodeIndex(dstIndex)
        .setDelimiter(sep)
      val mapping = lcc.transform(df)
      GraphIO.save(mapping, output)
    } else {
      val lcc = new GroupLccUndirected()
        .setPartitionNum(partitionNum)
        .setStorageLevel(storageLevel)
        .setBatchSize(batchSize)
        .setPullBatchSize(pullBatchSize)
        .setPSPartitionNum(psPartitionNum)
        .setInput(input)
        .setSrcNodeIndex(srcIndex)
        .setDstNodeIndex(dstIndex)
        .setDelimiter(sep)

      println(s"userGroupInput is not null, calc lcc by group.")
      val userGroup = GraphIO.load(userGroupInput, isWeighted=false, srcIndex = groupSrcIndex, dstIndex = groupDstIndex, sep=sep)
        .select("src", "dst").rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getLong(0), row.getLong(1)))
      lcc.setUserGroup(userGroup)

      val mapping = lcc.transform(df)
      GraphIO.save(mapping, output)
    }

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

//    // Add jvm parameters for executors
//    val executorJvmOptions = " -Djava.library.path=$JAVA_LIBRARY_PATH:/data/tdwadmin/tdwenv/tdwgaia/lib/native:/data/gaiaadmin/gaiaenv/tdwgaia/lib/native " +
//      "-verbose:gc -XX:-PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:<LOG_DIR>/gc.log " +
//      "-XX:+UseG1GC -XX:MaxGCPauseMillis=1000 -XX:G1HeapRegionSize=32M " +
//      "-XX:InitiatingHeapOccupancyPercent=60 -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 "
//
//    println(s"executorJvmOptions = ${executorJvmOptions}")
//    conf.set("spark.executor.extraJavaOptions", executorJvmOptions)

    conf.setMaster(mode)
    conf.setAppName("lccUndirected")
    val sc = SparkContext.getOrCreate(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}
