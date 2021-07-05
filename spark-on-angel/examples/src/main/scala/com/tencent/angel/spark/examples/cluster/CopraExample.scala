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

import com.tencent.angel.graph.community.copra.COPRA
import com.tencent.angel.graph.utils.GraphIO
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}


object CopraExample {

  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "local")

    val input = params.getOrElse("input", null)
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val batchSize = params.getOrElse("batchSize", "50").toInt
    val output = params.getOrElse("output", null)
    val src = params.getOrElse("src", "0").toInt
    val dst = params.getOrElse("dst", "1").toInt
    val weightIndex = params.getOrElse("weightIndex", "2").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum", "1").toInt
    val partitionNum = params.getOrElse("partitionNum", "1").toInt
    val useEdgeBalancePartition = params.getOrElse("useEdgeBalancePartition", "false").toBoolean
    val isWeighted = params.getOrElse("isWeighted", "false").toBoolean
    val arrayBoundsPath = params.getOrElse("arrayBoundsPath", null)
    val numMaxCommunities = params.getOrElse("numMaxCommunities", "5").toInt
    val preserveRate = params.getOrElse("preserveRate", "0.1").toFloat

    val sep = params.getOrElse("sep", "tab") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }
    val maxIteration = params.getOrElse("maxIteration", "3").toInt

    val sc = start(mode)
    val copra = new COPRA()
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")
      .setNumMaxCommunities(numMaxCommunities)
      .setBatchSize(batchSize)
      .setMaxIteration(maxIteration)
      .setPreserveRate(preserveRate)
      .setArrayBoundsPath(arrayBoundsPath)
      .setPartitionNum(partitionNum)
      .setIsWeighted(isWeighted)
      .setUseEdgeBalancePartition(useEdgeBalancePartition)

    val df = GraphIO.load(input, isWeighted = isWeighted, src, dst, weightIndex, sep = sep)

    val mapping = copra.transform(df)

    GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("copra")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //PSContext.getOrCreate(sc)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
