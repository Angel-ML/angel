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
import com.tencent.angel.spark.ml.graph.algo.feature.functionalEdgeAggregate.FunctionalEdgeAggregate
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.DataStatistics
import org.apache.spark.storage.StorageLevel

/**
 * Functional edge aggregate features on a directed (weighted) graph, cluster mode.
 *
 * Arguments (key:value):
 *   input, output, cpDir      required
 *   sep                       space/comma/tab/colon/bar, default space
 *   isWeighted                default true
 *   srcIndex, dstIndex, weightIndex  column index, default 0/1/2
 *   partitionNum              data partition number, default 100
 *   psPartitionNum            ps partition number, default spark.ps.instances or 10
 *   storageLevel              default MEMORY_ONLY
 *   batchSize, pullBatchSize, bufferSize, useBalancePartition  optional
 *   mode                      optional, overrides the master given by spark-submit
 */
object FunctionalEdgeAggregateExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val input = required(params, "input")
    val output = required(params, "output")
    val sc = start(params, "functionalEdgeAggregate")

    val sep = parseSep(params)
    val isWeighted = params.getOrElse("isWeighted", "true").toBoolean
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val weightIndex = params.getOrElse("weightIndex", "2").toInt
    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val psNum = psPartitionNum(params, sc)
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))

    val algo = new FunctionalEdgeAggregate()
      .setPartitionNum(partitionNum)
      .setPSPartitionNum(psNum)
      .setStorageLevel(storageLevel)
      .setIsWeighted(isWeighted)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")
      .setUseBalancePartition(params.getOrElse("useBalancePartition", "false").toBoolean)
    params.get("batchSize").foreach(v => algo.setBatchSize(v.toInt))
    params.get("pullBatchSize").foreach(v => algo.setPullBatchSize(v.toInt))
    params.get("bufferSize").foreach(v => algo.setBufferSize(v.toInt))

    val df = GraphIO.load(input, isWeighted = isWeighted, srcIndex = srcIndex, dstIndex = dstIndex,
      weightIndex = weightIndex, sep = sep)

    DataStatistics.setAppName(algo.getClass.getCanonicalName)
    DataStatistics.calcRDDSize(df, "angel.input.data")
    DataStatistics.setDataPartitionNum(partitionNum)
    DataStatistics.setPSPartitionNum(psNum)

    val result = algo.transform(df)
    GraphIO.save(result, output)
    stop()
  }
}
