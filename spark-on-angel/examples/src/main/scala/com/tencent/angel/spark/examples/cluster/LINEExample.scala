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
import com.tencent.angel.spark.ml.embedding.line.LINE
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.SparkUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

object LINEExample {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val input = params.getOrElse("input", "")
    val output = params.getOrElse("output", "")
    val oldModelInput = params.getOrElse("oldModelPath", "")

    val embeddingDim = params.getOrElse("embedding", "10").toInt
    val numNegSamples = params.getOrElse("negative", "5").toInt
    val numEpoch = params.getOrElse("epoch", "10").toInt

    val stepSize = params.getOrElse("stepSize", "0.1").toFloat
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum", "10").toInt
    val dataPartitionNum = params.getOrElse("dataPartitionNum", "100").toInt
    val withRemapping = params.getOrElse("remapping", "false").toBoolean
    val order = params.get("order").fold(2)(_.toInt)
    val saveModelInterval = params.getOrElse("saveModelInterval", "10").toInt
    val checkpointInterval = params.getOrElse("checkpointInterval", "2").toInt
    val saveMeta = params.getOrElse("saveMeta", "false").toBoolean

    val isWeight = params.getOrElse("isWeight", "false").toBoolean
    val sep = params.getOrElse("sep", "space") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }

    val sc = start()
    val numCores = SparkUtils.getNumCores(sc.getConf)
    // The number of partition is more than the cores. We do this to achieve dynamic load balance.
    var numDataPartitions = (numCores * 3.25).toInt
    if (dataPartitionNum > numDataPartitions) {
      numDataPartitions = dataPartitionNum
    }

    val line = new LINE()
      .setEmbedding(embeddingDim)
      .setNegative(numNegSamples)
      .setStepSize(stepSize)
      .setOrder(order)
      .setEpochNum(numEpoch)
      .setBatchSize(batchSize)
      .setPartitionNum(numDataPartitions)
      .setPSPartitionNum(psPartitionNum)
      .setIsWeighted(isWeight)
      .setRemapping(withRemapping)
      .setSaveModelInterval(saveModelInterval)
      .setCheckpointInterval(checkpointInterval)
      .setOutput(output)
      .setOldModelPath(oldModelInput)
      .setSaveMeta(saveMeta)

    val edges: DataFrame = GraphIO.load(input, isWeight, sep = sep)
    line.transform(edges)
    line.save(output, numEpoch, saveMeta)
    stop()
  }

  def start(): SparkContext = {
    val conf = new SparkConf()

    // Set specific parameters for LINE
    conf.set(AngelConf.ANGEL_PS_PARTITION_SOURCE_CLASS, classOf[PartitionSourceArray].getName)
    // Close the automatic checkpoint
    conf.set("spark.hadoop." + AngelConf.ANGEL_PS_BACKUP_AUTO_ENABLE, "false")
    conf.set("spark.hadoop." + AngelConf.ANGEL_PS_JVM_USE_PARALLEL_GC, "true")
    conf.set("spark.hadoop." + AngelConf.ANGEL_PS_JVM_PARALLEL_GC_USE_ADAPTIVE_SIZE, "false")
    conf.set("io.file.buffer.size", "16000000")
    conf.set("spark.hadoop.io.file.buffer.size", "16000000")

    // Add jvm parameters for executors
    val executorJvmOptions = " -verbose:gc -XX:-PrintGCDetails -XX:+PrintGCDateStamps -Xloggc:<LOG_DIR>/gc.log " +
      "-XX:+UseG1GC -XX:MaxGCPauseMillis=1000 -XX:G1HeapRegionSize=32M " +
      "-XX:InitiatingHeapOccupancyPercent=50 -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 "
    println(s"executorJvmOptions = $executorJvmOptions")
    conf.set("spark.executor.extraJavaOptions", executorJvmOptions)
    conf.setAppName("LINE")
    val sc = new SparkContext(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
