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
import com.tencent.angel.spark.ml.graph.algo.rank.pagerank.pagerankpro.PageRankPro
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.DataStatistics
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

// a risk propagation by pageRank

object PageRankProExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val sc = start(mode)

    val input = params.getOrElse("input", "") // (weighted) edge input
    val labelPosInput = params.getOrElse("labelPosInput", "")
    val partitionNum = params.getOrElse("partitionNum", "1").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val output = params.getOrElse("output", "")
    val psPartitionNum = params.getOrElse("psPartitionNum",
      sc.getConf.get("spark.ps.instances", "1")).toInt
    val tol = params.getOrElse("tol", "0.01").toFloat
    val resetProp = params.getOrElse("resetProp", "0.15").toFloat
    val isWeight = params.getOrElse("isWeight", "false").toBoolean
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val weightIndex = params.getOrElse("weightIndex", "2").toInt
    val useBalancePartition = params.getOrElse("useBalancePartition", "false").toBoolean
    val numBatch = params.getOrElse("numBatch", "1000").toInt
    val maxIteration = params.getOrElse("maxIteration", "200000").toInt
    val replicateEdges = params.getOrElse("replicateEdges", "false").toBoolean
    val equalPropagation = params.getOrElse("equalPropagation", "false").toBoolean
    val batchSize = params.getOrElse("batchSize", "1000").toInt

    val sep = params.getOrElse("sep", "tab") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }


    val edges = GraphIO.load(input, isWeighted = isWeight,
      srcIndex = srcIndex, dstIndex = dstIndex,
      weightIndex = weightIndex, sep = sep)

    //collect app name and data size
    val name = "PageRankPro"
    DataStatistics.setAppName(name)
    DataStatistics.calcRDDSize(edges, "angel.input.data")
    DataStatistics.setDataPartitionNum(partitionNum)
    DataStatistics.setPSPartitionNum(psPartitionNum)

    val label_pos = GraphIO.loadBiGraph(labelPosInput, isWeighted = false, sep = sep)
      .select("src", "dst").rdd
      .filter(row => !row.anyNull)
      .map(x => (x.getLong(0), x.getString(1).toFloat))

    val pagerank = new PageRankPro()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setTol(tol)
      .setResetProb(resetProp)
      .setIsWeighted(isWeight)
      .setUseBalancePartition(useBalancePartition)
      .setNumBatch(numBatch)
      .setMaxIteration(maxIteration)
      .setNeedReplicaEdge(replicateEdges)
        .setBatchSize(batchSize)

    pagerank.setPosLabelRDD(label_pos)
    pagerank.setEqualPropagation(equalPropagation)
    val ranks = pagerank.transform(edges)

    GraphIO.save(ranks, output)
    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()

    // Add jvm parameters for executors
//    var executorJvmOptions = conf.get("spark.executor.extraJavaOptions")
    //executorJvmOptions += " -XX:+UseG1GC -XX:MaxGCPauseMillis=1000 -XX:G1HeapRegionSize=32M " +
    //  "-XX:InitiatingHeapOccupancyPercent=50 -XX:ConcGCThreads=4 -XX:ParallelGCThreads=4 "
    //conf.set("spark.executor.extraJavaOptions", executorJvmOptions)
//    println(s"executorJvmOptions = ${executorJvmOptions}")

    conf.setMaster(mode)
    conf.setAppName("PageRank")
    val sc = new SparkContext(conf)
    //PSContext.getOrCreate(sc)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}
