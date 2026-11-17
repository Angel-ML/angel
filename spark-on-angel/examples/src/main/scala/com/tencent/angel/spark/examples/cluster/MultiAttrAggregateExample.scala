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
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.algo.rank.multiAttrAggregate.MultiAttrAggregate
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.DataStatistics
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object MultiAttrAggregateExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn")
    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output",null)
    val partitionNum = params.getOrElse("partitionNum","1").toInt
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val isWeighted = params.getOrElse("isWeighted", "false").toBoolean
    val weightIndex = params.getOrElse("weightIndex", "2").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "DISK_ONLY"))
    val batchSize = params.getOrElse("batchSize", "100").toInt
    val pullBatchSize = params.getOrElse("pullBatchSize", "100").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum", "1").toInt
    val useBalancePartition = params.getOrElse("useBalancePartition", "false").toBoolean
    val sep = params.getOrElse("sep", "tab") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }
    val iteration = params.getOrElse("iteration", "2").toInt

    // params for node attrs files
    // params for node attrs files, if has more than one directory, use comma to separate
    val nodeAttrPath = params.getOrElse("nodeAttrPath", null)

    val sc = start(mode)
    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
          .getOrElse(throw new Exception("checkpoint dir not provided"))
    sc.setCheckpointDir(cpDir)

    val aggregate = new MultiAttrAggregate()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setExtraInputs(nodeAttrPath.split(","))
      .setBatchSize(batchSize)
      .setUseBalancePartition(useBalancePartition)
      .setIsWeighted(isWeighted)
      .setMaxIteration(iteration)
      .setPullBatchSize(pullBatchSize)

    aggregate.setOutput(output)

    val df = GraphIO.load(input, isWeighted = isWeighted, srcIndex, dstIndex, weightIndex, sep = sep)

    //collect app name and data size
    DataStatistics.setAppName(aggregate.getClass.getCanonicalName)
    DataStatistics.calcRDDSize(df, "angel.input.data")
    DataStatistics.setDataPartitionNum(partitionNum)
    DataStatistics.setPSPartitionNum(psPartitionNum)

    val mapping = aggregate.transform(df)
    // output: two columns: node and aggregateResults,
    // aggregateResult is a string separated by space, every four values represents sum/mean/max/min for one attribute
//    GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String = "local"): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("hindex")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    //    sc.setLogLevel("WARN")
    //    sc.setCheckpointDir("cp")
    //    PSContext.getOrCreate(sc)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
