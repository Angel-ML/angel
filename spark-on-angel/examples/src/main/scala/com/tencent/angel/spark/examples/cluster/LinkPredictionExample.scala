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
import com.tencent.angel.spark.ml.graph.algo.intimacy.linkprediction.LinkPrediction
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.DataStatistics
import org.apache.spark.storage.StorageLevel

/**
 * Link prediction features of every edge (common friends, total friends, preferential
 * attachment, Adamic-Adar, resource allocation), cluster mode.
 *
 * Arguments (key:value):
 *   input, output, cpDir      required
 *   sep                       space/comma/tab/colon/bar, default space
 *   srcIndex, dstIndex        column index of src/dst, default 0/1
 *   partitionNum              data partition number, default 100
 *   psPartitionNum            ps partition number, default spark.ps.instances or 10
 *   storageLevel              default MEMORY_ONLY
 *   batchSize, pullBatchSize  default 1000/1000
 *   bufferSize                default 100000
 *   enableCheck               debug check, default false
 *   mode                      optional, overrides the master given by spark-submit
 */
object LinkPredictionExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val input = required(params, "input")
    val output = required(params, "output")
    val sc = start(params, "linkPrediction")

    val sep = parseSep(params)
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val psNum = psPartitionNum(params, sc)
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))

    val lp = new LinkPrediction()
      .setPartitionNum(partitionNum)
      .setPSPartitionNum(psNum)
      .setStorageLevel(storageLevel)
      .setBatchSize(params.getOrElse("batchSize", "1000").toInt)
      .setPullBatchSize(params.getOrElse("pullBatchSize", "1000").toInt)
      .setBufferSize(params.getOrElse("bufferSize", "100000").toInt)
      .setDebugMode(params.getOrElse("enableCheck", "false").toBoolean)
      .setInput(input)
      .setSrcNodeIndex(srcIndex)
      .setDstNodeIndex(dstIndex)

    val df = GraphIO.load(input, isWeighted = false, srcIndex = srcIndex, dstIndex = dstIndex,
      sep = sep)

    DataStatistics.setAppName(lp.getClass.getCanonicalName)
    DataStatistics.calcRDDSize(df, "angel.input.data")
    DataStatistics.setDataPartitionNum(partitionNum)
    DataStatistics.setPSPartitionNum(psNum)

    val result = lp.transform(df)
    GraphIO.save(result, output)
    stop()
  }
}
