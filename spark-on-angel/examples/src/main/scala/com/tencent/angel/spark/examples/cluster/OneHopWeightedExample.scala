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
import com.tencent.angel.spark.ml.graph.algo.statistics.clickrate.onehopweighted.OneHopWeighted
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.DataStatistics
import org.apache.spark.storage.StorageLevel

/**
 * Weighted one-hop click rate of ads on a weighted user-user graph. Cluster mode.
 *
 * Arguments (key:value):
 *   input        weighted user-user edges "user user weight", required
 *   extraInputs  user-ad clicks "user ad", required
 *   output, cpDir  required
 *   sep          space/comma/tab/colon/bar for all inputs, default space
 *   partitionNum data partition number, default 100
 *   psPartitionNum  ps partition number, default spark.ps.instances or 10
 *   storageLevel default MEMORY_ONLY
 *   batchSize, pullBatchSize  optional
 *   mode         optional, overrides the master given by spark-submit
 */
object OneHopWeightedExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val input = required(params, "input")
    val extraInputs = required(params, "extraInputs").split(",").map(_.trim).filter(_.nonEmpty)
    val output = required(params, "output")
    val sc = start(params, "oneHopWeighted")

    val sep = parseSep(params)
    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val psNum = psPartitionNum(params, sc)
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))

    val algo = new OneHopWeighted()
      .setDelimiter(sep)
      .setExtraInputs(extraInputs)
      .setPartitionNum(partitionNum)
      .setPSPartitionNum(psNum)
      .setStorageLevel(storageLevel)
    params.get("batchSize").foreach(v => algo.setBatchSize(v.toInt))
    params.get("pullBatchSize").foreach(v => algo.setPullBatchSize(v.toInt))

    val df = GraphIO.load(input, isWeighted = true, sep = sep)

    DataStatistics.setAppName(algo.getClass.getCanonicalName)
    DataStatistics.calcRDDSize(df, "angel.input.data")
    DataStatistics.setDataPartitionNum(partitionNum)
    DataStatistics.setPSPartitionNum(psNum)

    val result = algo.transform(df)
    GraphIO.save(result, output)
    stop()
  }
}
