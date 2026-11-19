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
import com.tencent.angel.spark.ml.graph.algo.connectedcomponent.wcc.WCC
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.DataStatistics
import org.apache.spark.storage.StorageLevel

/**
 * Weakly connected components, cluster mode.
 *
 * Arguments (key:value):
 *   input, output, cpDir      required
 *   sep                       space/comma/tab/colon/bar, default space
 *   srcIndex, dstIndex        column index of src/dst, default 0/1
 *   partitionNum              data partition number, default 100
 *   psPartitionNum            ps partition number, default spark.ps.instances or 10
 *   storageLevel              default MEMORY_ONLY
 *   batchSize, pullBatchSize, bufferSize, useBalancePartition, balancePartitionPercent  optional
 *   mode                      optional, overrides the master given by spark-submit
 */
object WCCExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val input = required(params, "input")
    val output = required(params, "output")
    val sc = start(params, "wcc")

    val sep = parseSep(params)
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val psNum = psPartitionNum(params, sc)
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))

    val wcc = new WCC()
      .setPartitionNum(partitionNum)
      .setPSPartitionNum(psNum)
      .setStorageLevel(storageLevel)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")
      .setUseBalancePartition(params.getOrElse("useBalancePartition", "false").toBoolean)
      .setBalancePartitionPercent(params.getOrElse("balancePartitionPercent", "0.7").toFloat)
    params.get("batchSize").foreach(v => wcc.setBatchSize(v.toInt))
    params.get("pullBatchSize").foreach(v => wcc.setPullBatchSize(v.toInt))
    params.get("bufferSize").foreach(v => wcc.setBufferSize(v.toInt))

    val df = GraphIO.load(input, isWeighted = false, srcIndex = srcIndex, dstIndex = dstIndex,
      sep = sep)

    DataStatistics.setAppName(wcc.getClass.getCanonicalName)
    DataStatistics.calcRDDSize(df, "angel.input.data")
    DataStatistics.setDataPartitionNum(partitionNum)
    DataStatistics.setPSPartitionNum(psNum)

    val mapping = wcc.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }
}
