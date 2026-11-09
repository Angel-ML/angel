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
import com.tencent.angel.spark.ml.graph.algo.rank.commonFriendsRecall._
import com.tencent.angel.spark.ml.graph.utils.{Delimiter, GraphIO}
import com.tencent.angel.spark.ml.util.DataStatistics
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/*
Common friends recall for TME kg group
*/

object CommonFriendsRecallExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val sc = start(mode)

    val input = params.getOrElse("input", null)
    val sep = Delimiter.parse(params.getOrElse("sep", Delimiter.TAB))
    val output = params.getOrElse("output", null)
    val partitionNum = params.getOrElse("partitionNum", "1").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum", sc.getConf.get("spark.ps.instances", "1")).toInt
    val useBalancePartition = params.getOrElse("useBalancePartition", "false").toBoolean
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val pullBatchSize = params.getOrElse("pullBatchSize", "1000").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    sc.setCheckpointDir(cpDir)
    val top = params.getOrElse("top", "0").toInt
    val breakUpSave = params.getOrElse("breakUpSave", "false").toBoolean

    // optional choices: friends / followers / followers1
    val method = params.getOrElse("method", "friends")
    val outputLists = params.getOrElse("outputLists", "false").toBoolean

    start(mode)
    val startTime = System.currentTimeMillis()
    val recall = if (method == "friends") {
      new CommonFriendsRecall()
        .setPartitionNum(partitionNum)
        .setStorageLevel(storageLevel)
        .setBatchSize(batchSize)
        .setPullBatchSize(pullBatchSize)
        .setPSPartitionNum(psPartitionNum)
        .setTopK(top)
        .setUseBalancePartition(useBalancePartition)
    } else if (method == "followers"){ // fans' common followers
      new CommonFollowersRecall()
        .setPartitionNum(partitionNum)
        .setStorageLevel(storageLevel)
        .setBatchSize(batchSize)
        .setPullBatchSize(pullBatchSize)
        .setPSPartitionNum(psPartitionNum)
        .setTopK(top)
        .setUseBalancePartition(useBalancePartition)
    } else if (method == "pathCounting"){ // path counting
      new PathCountingRecall()
        .setPartitionNum(partitionNum)
        .setStorageLevel(storageLevel)
        .setBatchSize(batchSize)
        .setPullBatchSize(pullBatchSize)
        .setPSPartitionNum(psPartitionNum)
        .setTopK(top)
        .setUseBalancePartition(useBalancePartition)
    } else{                            // commonFollowers
      new CommonFollowersRecall1()
        .setPartitionNum(partitionNum)
        .setStorageLevel(storageLevel)
        .setBatchSize(batchSize)
        .setPullBatchSize(pullBatchSize)
        .setPSPartitionNum(psPartitionNum)
        .setTopK(top)
        .setBreakUpSave(breakUpSave)
        .setUseBalancePartition(useBalancePartition)
        .setOutputLists(outputLists)
    }

    val df = GraphIO.load(input, isWeighted = false,
      srcIndex = srcIndex, dstIndex = dstIndex, sep = sep)

    //collect app name and data size
    DataStatistics.setAppName(recall.getClass.getCanonicalName)
    DataStatistics.calcRDDSize(df, "angel.input.data")
    DataStatistics.setDataPartitionNum(partitionNum)
    DataStatistics.setPSPartitionNum(psPartitionNum)

    val mapping = recall.transform(df)
    GraphIO.save(mapping, output)

    println(s"cost ${System.currentTimeMillis() - startTime} ms")
    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()

    conf.setMaster(mode)
    conf.setAppName("commonFriendsRecall")
    val sc = SparkContext.getOrCreate(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}
