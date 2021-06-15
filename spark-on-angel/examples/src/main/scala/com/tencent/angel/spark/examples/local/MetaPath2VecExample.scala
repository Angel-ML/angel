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
package com.tencent.angel.spark.examples.local

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.graph.embedding.metapath2vec._
import com.tencent.angel.graph.utils.GraphIO
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

// a deep walker for heterogenous and unweighted graph
// clear the output dir before running

object MetaPath2VecExample {
  def main(args: Array[String]): Unit = {

    val mode = "local"
    val input = "data/bc/edge"
    val output = "model/metapath"
    val partitionNum = 1
    val storageLevel = StorageLevel.MEMORY_ONLY
    val psPartitionNum = 1
    val batchSize = 1000
    val pullBatchSize = 1000
    var metaPath = "0-1-2-1-0" // should be symmetrical, eg: 0-0, 0-1-0, 0-1-2-1-0
    val nodeTypePath = null
    val walkLength = 5
    val numWalks = 1

    // read and set metaPath
    if (nodeTypePath == null) {
      println(s"nodeTypePath is null, a random walk without metaPath is used.")
      println(s"set metaPath to 0-0.")
      metaPath = "0-0"
    } else {
      assert(metaPath != null, s"input metaPath is null.")
      val meta = metaPath.trim.split("-").map(_.toInt)
      assert(meta.length > 1, s"metaPath should be symmetrical, eg.0-1-2-1-0")
      if (meta.length < 5) {
        println(s"for homogeneous or bipartite graph, it is recommended to use DeepWalk.")
      }
    }

    start(mode)
    val metaPath2Vec = new MetaPath2Vec()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setBatchSize(batchSize)
      .setWalkLength(walkLength)
      .setPullBatchSize(pullBatchSize)
      .setEpochNum(numWalks)

    metaPath2Vec.setOutputDir(output)
    metaPath2Vec.setMetaPath(metaPath)

    // read and set nodeType
    if (nodeTypePath != null) {
      val nodeAttrs = GraphIO.load(nodeTypePath, isWeighted = false, sep = " ")
        .select("src", "dst").rdd
        .map(row => (row.getLong(0), row.getLong(1).toInt+1))
        .distinct(partitionNum)
      metaPath2Vec.setNodeAttr(nodeAttrs)
    }

    val df = GraphIO.load(input, isWeighted = false, sep = " ")
    val mapping = metaPath2Vec.transform(df)
    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()

    conf.setMaster(mode)
    conf.setAppName("metaPath2Vec")
    val sc = new SparkContext(conf)
    //PSContext.getOrCreate(sc)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
