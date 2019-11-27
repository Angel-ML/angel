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
import com.tencent.angel.spark.ml.graph.louvain.Louvain
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object LouvainExample {
  def main(args: Array[String]): Unit = {

    val params = ArgsUtil.parse(args)
    val sc = start()

    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output", null)
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum",
      sc.getConf.get("spark.ps.instances", "10")).toInt
    val numFold = params.getOrElse("numFold", "3").toInt
    val numOpt = params.getOrElse("numOpt", "4").toInt
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val isWeighted = params.getOrElse("isWeighted", "false").toBoolean
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val weightIndex = params.getOrElse("weightIndex", "2").toInt
    val enableCheck = params.getOrElse("enableCheck", "false").toBoolean
    val eps = params.getOrElse("eps", "0.0").toDouble
    val bufferSize = params.getOrElse("bufferSize", "1000000").toInt

    val sep = params.getOrElse("sep",  "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
    .getOrElse(throw new Exception("checkpoint dir not provided"))
    sc.setCheckpointDir(cpDir)

    var exitCode = 0
    try {
      val louvain = new Louvain()
        .setPartitionNum(partitionNum)
        .setPSPartitionNum(psPartitionNum)
        .setStorageLevel(storageLevel)
        .setNumFold(numFold)
        .setNumOpt(numOpt)
        .setBatchSize(batchSize)
        .setDebugMode(enableCheck)
        .setEps(eps)
        .setBufferSize(bufferSize)
        .setIsWeighted(isWeighted)

      val df = GraphIO.load(input, isWeighted = isWeighted,
        srcIndex = srcIndex, dstIndex = dstIndex,
        weightIndex = weightIndex, sep = sep)
      val mapping = louvain.transform(df)
      GraphIO.save(mapping, output)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        exitCode = -1
    } finally {
      stop()
      System.exit(exitCode)
    }
  }

  def start(): SparkContext = {
    val conf = new SparkConf()
    conf.setAppName("louvain")
    val sc = new SparkContext(conf)
    PSContext.getOrCreate(sc)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}