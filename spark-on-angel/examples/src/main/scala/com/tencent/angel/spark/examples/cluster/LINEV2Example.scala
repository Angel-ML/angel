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
import com.tencent.angel.spark.ml.graph.algo.embedding.line2.LINE
import com.tencent.angel.spark.ml.util.DataStatistics

/**
 * LINE (v2, `algo.embedding.line2`) graph embedding, cluster mode. Supports int node ids or
 * arbitrary string node ids with `remapping:true` (and `postMapping:true` to write the original
 * ids back into the embedding output).
 *
 * The final model is saved to `$output/CP_$epoch`; intermediate models are saved every
 * `saveModelInterval` epochs.
 *
 * Arguments (key:value):
 *   input, output, cpDir      required
 *   sep                       space/comma/tab/colon/bar, default space
 *   isWeighted                default false
 *   srcIndex, dstIndex, weightIndex  column index, default 0/1/2
 *   embedding, negative, stepSize, order, epoch, batchSize  default 128/5/0.025/2/10/1000
 *   partitionNum              data partition number, default 100
 *   psPartitionNum            ps partition number, default spark.ps.instances or 10
 *   remapping, postMapping    default false/false
 *   subSample                 default false
 *   checkpointInterval, saveModelInterval  default 5/5
 *   saveOutputEmbedding       also save the context embedding (order 2 only), default false
 *   extraInputEmbeddingPath, extraOutputEmbeddingPath  init embedding paths, default empty
 *   useBalancePartition       default false
 *   mode                      optional, overrides the master given by spark-submit
 */
object LINEV2Example {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val input = required(params, "input")
    val output = required(params, "output")
    val sc = start(params, "lineV2")

    val sep = parseSep(params)
    val isWeighted = params.getOrElse("isWeighted", "false").toBoolean
    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val psNum = psPartitionNum(params, sc)
    val epoch = params.getOrElse("epoch", "10").toInt
    val saveOutputEmbedding = params.getOrElse("saveOutputEmbedding", "false").toBoolean

    val line = new LINE()
      .setEmbedding(params.getOrElse("embedding", "128").toInt)
      .setNegative(params.getOrElse("negative", "5").toInt)
      .setStepSize(params.getOrElse("stepSize", "0.025").toFloat)
      .setOrder(params.getOrElse("order", "2").toInt)
      .setEpochNum(epoch)
      .setBatchSize(params.getOrElse("batchSize", "1000").toInt)
      .setPartitionNum(partitionNum)
      .setPSPartitionNum(psNum)
      .setIsWeighted(isWeighted)
      .setSrcNodeIdCol("src")
      .setDstNodeIdCol("dst")
      .setWeightCol("weight")
      .setRemapping(params.getOrElse("remapping", "false").toBoolean)
      .setPostMapping(params.getOrElse("postMapping", "false").toBoolean)
      .setSubSample(params.getOrElse("subSample", "false").toBoolean)
      .setCheckpointInterval(params.getOrElse("checkpointInterval", "5").toInt)
      .setSaveModelInterval(params.getOrElse("saveModelInterval", "5").toInt)
      .setSaveOutputEmbedding(saveOutputEmbedding)
      .setExtraInputEmbeddingPath(params.getOrElse("extraInputEmbeddingPath", ""))
      .setExtraOutputEmbeddingPath(params.getOrElse("extraOutputEmbeddingPath", ""))
      .setUseBalancePartition(params.getOrElse("useBalancePartition", "false").toBoolean)
      .setOutput(output)

    val df = loadStringEdges(input, sep, isWeighted,
      params.getOrElse("srcIndex", "0").toInt,
      params.getOrElse("dstIndex", "1").toInt,
      params.getOrElse("weightIndex", "2").toInt)

    DataStatistics.setAppName(line.getClass.getCanonicalName)
    DataStatistics.calcRDDSize(df, "angel.input.data")
    DataStatistics.setDataPartitionNum(partitionNum)
    DataStatistics.setPSPartitionNum(psNum)

    line.transform(df)
    line.save(output, epoch, saveOutputEmbedding)
    stop()
  }
}
