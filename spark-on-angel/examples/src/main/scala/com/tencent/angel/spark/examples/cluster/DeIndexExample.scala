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
import com.tencent.angel.spark.ml.graph.utils.partitionstrategy.reindexpartition.DeIndex
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.DataStatistics
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object DeIndexExample {


  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "local")
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
    val df = GraphIO.loadInt(input, isWeighted = false, srcIndex, dstIndex, sep = sep)
    val edges = df.select("src", "dst").rdd
      .map(row => (row.getInt(0), row.getInt(1)))

    edges.persist(StorageLevel.DISK_ONLY)
    println(edges.take(10).mkString(","))

    //collect app name and data size
    DataStatistics.setAppName("com.tencent.angel.spark.ml.graph.utils.partitionstrategy.reindexpartition.DeIndex")
    DataStatistics.calcRDDSize(df, "angel.input.data")
    DataStatistics.setDataPartitionNum(partitionNum)
    DataStatistics.setPSPartitionNum(psPartitionNum)

    val beforeStartPS = System.currentTimeMillis()
    PSContext.getOrCreate(SparkContext.getOrCreate())
    println(s"starting ps cost ${System.currentTimeMillis() - beforeStartPS} ms")

    val deIndex = new DeIndex()
    deIndex.index(nodesMapPath, psPartitionNum, batchSize)
    val deIndexedEdges = deIndex.decodeEdge(edges, batchSize)

    val retRDD = deIndexedEdges.map(f => Row.fromSeq(Seq[Any](f._1, f._2)))
    def transformSchema(schema: StructType): StructType = {
      StructType(Seq(
        StructField(s"${
          "srcId"
        }", LongType, nullable = false),
        StructField(s"${
          "dstId"
        }", LongType, nullable = false)
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
    //    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    //    sc.setLogLevel("WARN")
    //        sc.setCheckpointDir("cp")
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
