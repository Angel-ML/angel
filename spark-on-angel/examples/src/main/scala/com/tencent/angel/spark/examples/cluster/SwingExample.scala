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
import com.tencent.angel.graph.rank.swing.Swing
import com.tencent.angel.graph.utils.{Delimiter, GraphIO}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/*
if item ids are strings, set reindexItem param to true
to execute reindexing and decoding procedures.
*/

object SwingExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val sc = start(mode)

    val input = params.getOrElse("input", null)
    val sep = Delimiter.parse(params.getOrElse("sep", Delimiter.TAB))
    val output = params.getOrElse("output", null)
    val partitionNum = params.getOrElse("partitionNum", "1").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum",
      sc.getConf.get("spark.ps.instances", "1")).toInt
    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val pullBatchSize = params.getOrElse("pullBatchSize", "200").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val alpha = params.getOrElse("alpha", "0").toFloat
    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    sc.setCheckpointDir(cpDir)

    // set [topFrom, topTo) to calc scores between less popular items and other items
    // an item's popularity depends on it's num of links to users
    val topFrom = params.getOrElse("topFrom", "0").toInt
    val topTo = params.getOrElse("topTo", "0").toInt
    val beta = params.getOrElse("beta", "5").toInt
    val gamma = params.getOrElse("gamma", "-0.3").toFloat
    val superItemThreshold = params.getOrElse("superItemThreshold", "2000").toInt
    val superItemPairBatch = params.getOrElse("superItemPairBatch", "800").toInt

    start(mode)
    val startTime = System.currentTimeMillis()
    val swing = new Swing()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setBatchSize(batchSize)
      .setPullBatchSize(pullBatchSize)
      .setPSPartitionNum(psPartitionNum)
      .setDelta(alpha)
      .setBeta(beta)
      .setGamma(gamma)
      .setSuperItemThreshold(superItemThreshold)
    swing.setItemFrom(topFrom)
    swing.setItemTo(topTo)
    swing.setSuperItemPairBatch(superItemPairBatch)

    val df = GraphIO.load(input, isWeighted = false,
      srcIndex = srcIndex, dstIndex = dstIndex, sep = sep)

    val mapping = swing.transform(df)
    GraphIO.save(mapping, output)

    println(s"cost ${System.currentTimeMillis() - startTime} ms")
    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()

    conf.setMaster(mode)
    conf.setAppName("swing")
    val sc = SparkContext.getOrCreate(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
