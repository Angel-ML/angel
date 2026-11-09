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

import com.tencent.angel.spark.examples.cluster.ClusterExampleUtils._
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.algo.communitydetection.louvainRepair.LouvainOpt
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.DataStatistics
import org.apache.spark.storage.StorageLevel

/**
 * Louvain with community merge / repair optimization, cluster mode.
 *
 * Arguments (key:value):
 *   input, output, cpDir      required
 *   sep                       space/comma/tab/colon/bar, default space
 *   isWeighted                default false
 *   srcIndex, dstIndex, weightIndex  column index, default 0/1/2
 *   partitionNum              data partition number, default 100
 *   psPartitionNum            ps partition number, default spark.ps.instances or 10
 *   storageLevel              default MEMORY_ONLY
 *   numFold, numOpt           default 3/4
 *   batchSize, bufferSize     default 10000/1000000
 *   eps                       default 0.0
 *   enableCheck               debug check, default false
 *   preserveRate              default 0.0
 *   useMergeStrategy          default true
 *   useBalancePartition, balancePartitionPercent  default false/0.7
 *   mode                      optional, overrides the master given by spark-submit
 */
object LouvainOptExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val input = required(params, "input")
    val output = required(params, "output")
    val sc = start(params, "louvainOpt")

    val sep = parseSep(params)
    val isWeighted = params.getOrElse("isWeighted", "false").toBoolean
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val weightIndex = params.getOrElse("weightIndex", "2").toInt
    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val psNum = psPartitionNum(params, sc)
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))

    val louvain = new LouvainOpt()
      .setPartitionNum(partitionNum)
      .setPSPartitionNum(psNum)
      .setStorageLevel(storageLevel)
      .setIsWeighted(isWeighted)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")
      .setNumFold(params.getOrElse("numFold", "3").toInt)
      .setNumOpt(params.getOrElse("numOpt", "4").toInt)
      .setBatchSize(params.getOrElse("batchSize", "10000").toInt)
      .setBufferSize(params.getOrElse("bufferSize", "1000000").toInt)
      .setEps(params.getOrElse("eps", "0.0").toDouble)
      .setDebugMode(params.getOrElse("enableCheck", "false").toBoolean)
      .setPreserveRate(params.getOrElse("preserveRate", "0.0").toDouble)
      .setUseMergeStrategy(params.getOrElse("useMergeStrategy", "true").toBoolean)
      .setUseBalancePartition(params.getOrElse("useBalancePartition", "false").toBoolean)
      .setBalancePartitionPercent(params.getOrElse("balancePartitionPercent", "0.7").toFloat)

    val df = GraphIO.load(input, isWeighted = isWeighted, srcIndex = srcIndex, dstIndex = dstIndex,
      weightIndex = weightIndex, sep = sep)

    DataStatistics.setAppName(louvain.getClass.getCanonicalName)
    DataStatistics.calcRDDSize(df, "angel.input.data")
    DataStatistics.setDataPartitionNum(partitionNum)
    DataStatistics.setPSPartitionNum(psNum)

    val mapping = louvain.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }
}
