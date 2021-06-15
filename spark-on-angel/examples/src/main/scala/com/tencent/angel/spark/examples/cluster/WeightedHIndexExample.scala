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
import com.tencent.angel.graph.rank.weightedHIndex.WeightedHIndex
import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object WeightedHIndexExample {


  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", null)
    val partitionNum = params.getOrElse("partitionNum","1").toInt
    val srcIndex = params.getOrElse("src", "0").toInt
    val dstIndex = params.getOrElse("dst", "1").toInt
    val weightIndex = params.getOrElse("weightIndex", "2").toInt
    val storageLevel = StorageLevel.MEMORY_ONLY
    val sc = start(mode)
    val psPartitionNum = params.getOrElse("psPartitionNum",
      sc.getConf.get("spark.ps.instances", "1")).toInt
    val isWeighted = params.getOrElse("isWeighted", "false").toBoolean
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val pullBatchSize = params.getOrElse("pullBatchSize", "1000").toInt
    val cpDir = params.getOrElse("cpDir", GraphIO.defaultCheckpointDir.getOrElse(throw new Exception("checkpoint dir not provided")))
    sc.setCheckpointDir(cpDir)
    val sep = params.getOrElse("sep", "tab") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val whindex = new WeightedHIndex()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setIsWeighted(isWeighted)
      .setBatchSize(batchSize)
      .setPullBatchSize(pullBatchSize)

    val df = GraphIO.load(input, isWeighted = isWeighted, srcIndex, dstIndex, weightIndex, sep = sep)
    val mapping = whindex.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String = "local"): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("hindex")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
