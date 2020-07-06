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
import com.tencent.angel.graph.rank.closeness.Closeness
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.graph.utils.{Delimiter, GraphIO}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object ClosenessExample {
  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val sc = start(mode)

    val input = params.getOrElse("input", "")
    val output = params.getOrElse("output", null)
    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum",
      sc.getConf.get("spark.ps.instances", "10")).toInt
    val msgNumBatch = params.getOrElse("msgNumBatch", "4").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))

    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val maxIter = params.getOrElse("maxIter", "100").toInt
    val useBalancePartition = params.getOrElse("useBalancePartition", "false").toBoolean
    val percent = params.getOrElse("balancePartitionPercent", "0.7").toFloat

    val p = params.getOrElse("p", "6").toInt
    val verboseSaving = params.getOrElse("verboseSaving", "true").toBoolean

    val isDirected = params.getOrElse("isDirected", "true").toBoolean
    val sep = Delimiter.parse(params.getOrElse("sep",Delimiter.SPACE))


    val closeness = new Closeness()
      .setPartitionNum(partitionNum)
      .setPSPartitionNum(psPartitionNum)
      .setMsgNumBatch(msgNumBatch)
      .setMaxIter(maxIter)
      .setStorageLevel(storageLevel)
      .setP(p)
      .setVerboseSaving(verboseSaving)
      .setUseBalancePartition(useBalancePartition)
      .setBalancePartitionPercent(percent)
      .setIsDirected(isDirected)

    val df = GraphIO.load(input, false,
      srcIndex = srcIndex, dstIndex = dstIndex, sep = sep)

    val mapping = closeness.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("AngelCloseness")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    PSContext.getOrCreate(sc)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}
