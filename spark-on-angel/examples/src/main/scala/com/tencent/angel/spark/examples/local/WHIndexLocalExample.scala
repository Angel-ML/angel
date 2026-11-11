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

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.algo.rank.whindex.WHIndex
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object WHIndexLocalExample {


  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "local")
    val input = params.getOrElse("input", "data/bc/edge")
    val output = params.getOrElse("output", "model/whindex")
    val partitionNum = params.getOrElse("partitionNum","1").toInt
    val unDirected = params.getOrElse("unDirected", "false").toBoolean
    val minId = params.getOrElse("minId", "1").toLong
    val maxId = params.getOrElse("maxId", "11").toLong
    val srcIndex = params.getOrElse("src", "0").toInt
    val dstIndex = params.getOrElse("dst", "1").toInt
    val storageLevel = StorageLevel.MEMORY_ONLY
    val batchSize = 100
    val sc = start(mode)
    val psPartitionNum = params.getOrElse("psPartitionNum",
      sc.getConf.get("spark.ps.instances", "10")).toInt
    val useBalancePartition = params.getOrElse("useBalancePartition", "true").toBoolean

    val cpDir = "cp"
    sc.setCheckpointDir(cpDir)
    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val hindex = new WHIndex()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setUnDirected(unDirected)
      .setUseBalancePartition(useBalancePartition)

    val df = GraphIO.load(input, isWeighted = false, srcIndex, dstIndex, sep = sep)

    //collect app name and data size

    val mapping = hindex.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String = "local"): SparkContext = {
    val conf = new SparkConf()
    var executorJvmOptions = conf.get("spark.executor.extraJavaOptions", "")
    executorJvmOptions += " -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 -Xss4M "
    conf.set("spark.executor.extraJavaOptions", executorJvmOptions)
    println(s"executorJvmOptions = ${executorJvmOptions}")
    conf.setMaster(mode)
    conf.setAppName("hindex")
//    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
//    sc.setCheckpointDir("cp")
//    PSContext.getOrCreate(sc)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
