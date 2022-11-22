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
import com.tencent.angel.graph.statistics.lccUndirected.{GroupLccUndirected, LccUndirected}
import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.ps.storage.matrix.PartitionSourceArray
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object LccUndirectedLocalExample {

  def main(args: Array[String]): Unit = {

    val mode = "local"
    val input = "data/bc/karate_club_network.txt"
    val output = "data/output/lcc"
    val partitionNum = 1
    val storageLevel = StorageLevel.MEMORY_ONLY
    val batchSize = 5
    val psPartitionNum = 1

    val userGroupInput = null
    val sep = " "

    val srcIndex = 0
    val dstIndex = 1
    val pullBatchSize = 5
    val groupSrcIndex = 0
    val groupDstIndex = 1
    val sc = start(mode)
    val cpDir = "data/cp"
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
      mapping.show(5)
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
      val userGroup = GraphIO.load(userGroupInput, isWeighted = false, srcIndex = groupSrcIndex, dstIndex = groupDstIndex, sep = sep)
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
