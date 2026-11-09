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

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import com.tencent.angel.spark.ml.graph.utils.partitionstrategy.reindexpartition.ReIndex
import com.tencent.angel.spark.ml.graph.utils.{DistinctNodes, GraphIO}
import com.tencent.angel.spark.ml.util.DataStatistics
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object ReIndexExample {


  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output",null)
    val nodesMapPath = params.getOrElse("nodesMapPath", null)
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val batchSize = params.getOrElse("batchSize", "1000").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum", "1").toInt
    val partitionNum = params.getOrElse("partitionNum", "1").toInt
    val sep = params.getOrElse("sep", "tab") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }
    val srcIndex = params.getOrElse("src", "0").toInt
    val dstIndex = params.getOrElse("dst", "1").toInt

    start(mode)

    // load edges
    val df = GraphIO.load(input, isWeighted = false, srcIndex, dstIndex, sep = sep)
    val beforeData = System.currentTimeMillis()
    val edges = df.select("src", "dst").rdd
        .map(row => (row.getLong(0), row.getLong(1)))
    edges.persist(StorageLevel.DISK_ONLY)
    println(edges.take(10).mkString(","))

    // distinct
    val nodesSet = DistinctNodes(edges, partitionNum)

    nodesSet.persist(storageLevel)
    val maxId = nodesSet.max() + 1
    val minId = nodesSet.min()
    val numNodes = nodesSet.count()

    println(s"minId: $minId, maxId: $maxId, num of edges: ${edges.count}, num of nodes: $numNodes}")
    println(s"loading distinct nodeSet cost ${System.currentTimeMillis() - beforeData} ms.")

    //collect app name and data size
    DataStatistics.setAppName("com.tencent.angel.spark.ml.graph.utils.partitionstrategy.reindexpartition.ReIndex")
    DataStatistics.calcRDDSize(df, "angel.input.data")
    DataStatistics.setDataPartitionNum(partitionNum)
    DataStatistics.setPSPartitionNum(psPartitionNum)

    val beforeStartPS = System.currentTimeMillis()
    PSContext.getOrCreate(SparkContext.getOrCreate())
    println(s"starting ps cost ${System.currentTimeMillis() - beforeStartPS} ms")

    val beforeInitPs = System.currentTimeMillis()
    val reIndex = new ReIndex()
    reIndex.index(psPartitionNum, minId, maxId, nodesSet, numNodes, batchSize, nodesMapPath)
    println(s"pushing to ps cost ${System.currentTimeMillis() - beforeInitPs} ms")

    val beforeEncode = System.currentTimeMillis()
    val reIndexedEdges = reIndex.encode(edges, batchSize, partitionNum)
    println(s"encoding cost ${System.currentTimeMillis() - beforeEncode} ms")

    val retRDD = reIndexedEdges.map(f => Row.fromSeq(Seq[Any](f._1, f._2)))
    def transformSchema(schema: StructType): StructType = {
      StructType(Seq(
        StructField(s"${
          "srcId"
        }", IntegerType, nullable = false),
        StructField(s"${
          "dstId"
        }", IntegerType, nullable = false)
      ))
    }
    val result = df.sparkSession.createDataFrame(retRDD, transformSchema(df.schema))

    GraphIO.save(result, output)
    stop()
  }

  def start(mode: String = "local"): Unit = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("reindex")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
//        sc.setCheckpointDir("cp")
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
