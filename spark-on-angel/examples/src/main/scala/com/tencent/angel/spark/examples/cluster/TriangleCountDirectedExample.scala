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
import com.tencent.angel.spark.ml.graph.algo.statistics.triangle.{GroupDirectedTriangle, TriangleCountingDirected}
import com.tencent.angel.spark.ml.graph.utils.{Delimiter, GraphIO}
import com.tencent.angel.spark.ml.util.DataStatistics
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

// edges must be distinct
object TriangleCountDirectedExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn")
    val sc = start(mode)

    val input = params.getOrElse("input", null)
    val userGroupInput = params.getOrElse("userGroupInput", null)
    val sep = Delimiter.parse(params.getOrElse("sep", Delimiter.SPACE))
    val output = params.getOrElse("output", null)

    val partitionNum = params.getOrElse("partitionNum", "100").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum",
      sc.getConf.get("spark.ps.instances", "10")).toInt

    val batchSize = params.getOrElse("batchSize", "10000").toInt
    val pullBatchSize = params.getOrElse("pullBatchSize", "1000").toInt

    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val enableCheck = params.getOrElse("enableCheck", "false").toBoolean
    val bufferSize = params.getOrElse("bufferSize", "1000000").toInt

    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val groupSrcIndex = params.getOrElse("groupSrcIndex", "0").toInt
    val groupDstIndex = params.getOrElse("groupDstIndex", "1").toInt
    val hasEdgeType = params.getOrElse("hasEdgeType", "false").toBoolean // is only useful for group triangle counting
    val edgeTypeIndex = params.getOrElse("edgeTypeIndex", "2").toInt

    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    sc.setCheckpointDir(cpDir)

    start(mode)
    val startTime = System.currentTimeMillis()
    val df = GraphIO.load(input, isWeighted = false, srcIndex=srcIndex, dstIndex=dstIndex, sep=sep,
      hasEdgeType=hasEdgeType, edgeTypeIndex=edgeTypeIndex)

    //collect app name and data size
//    DataStatistics.calcRDDSize(df, "angel.input.data")
    DataStatistics.setDataPartitionNum(partitionNum)
    DataStatistics.setPSPartitionNum(psPartitionNum)
    if (userGroupInput == null) {
      // compute directed triangles for every node
      // the output is arranged as "nodeID | number_of_7_triangles"
      val triangleCount = new TriangleCountingDirected()
        .setPartitionNum(partitionNum)
        .setStorageLevel(storageLevel)
        .setBatchSize(batchSize)
        .setPullBatchSize(pullBatchSize)
        .setDebugMode(enableCheck)
        .setBufferSize(bufferSize)
        .setPSPartitionNum(psPartitionNum)
        .setInput(input)
        .setSrcNodeIndex(srcIndex)
        .setDstNodeIndex(dstIndex)
        .setDelimiter(sep)
      DataStatistics.setAppName(triangleCount.getClass.getCanonicalName)
      val mapping = triangleCount.transform(df)
      GraphIO.save(mapping, output)
    } else {
      // compute directed triangles for every group, edge types should be considered if "hasEdgeType" is true
      // the output is arranged as "groupID | (edgeType) | number_of_7_triangles"
      val triangle = new GroupDirectedTriangle()
        .setPartitionNum(partitionNum)
        .setStorageLevel(storageLevel)
        .setBatchSize(batchSize)
        .setPullBatchSize(pullBatchSize)
        .setPSPartitionNum(psPartitionNum)
        .setInput(input)
        .setSrcNodeIndex(srcIndex)
        .setDstNodeIndex(dstIndex)
        .setDelimiter(sep)
      DataStatistics.setAppName(triangle.getClass.getCanonicalName)
      triangle.setOutputDir(output)
      triangle.setHasEdgeType(hasEdgeType)

      println(s"userGroupInput is not null, calc triangles by group.")
      val userGroup = GraphIO.load(userGroupInput, isWeighted=false, srcIndex=groupSrcIndex, dstIndex=groupDstIndex, sep=sep)
        .select("src", "dst").rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getLong(0), row.getLong(1)))
      triangle.setUserGroup(userGroup)

      val mapping = triangle.transform(df)
    }

    println(s"cost ${System.currentTimeMillis() - startTime} ms")
    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("triangle_count_directed")
    val sc = SparkContext.getOrCreate(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}
