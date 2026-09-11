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
import com.tencent.angel.graph.community.egonetwork.{EgoNetwork, EgoNetworkV2}
import com.tencent.angel.graph.utils.{Delimiter, GraphIO}
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.core.ArgsUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object EgoNetworkExample {

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
    val pullBatchSize = params.getOrElse("pullBatchSize", "1000").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    val nodePath = params.getOrElse("nodePath", null)
    val sepInNodePath = Delimiter.parse(params.getOrElse("sepInNodePath", Delimiter.SPACE))
    sc.setCheckpointDir(cpDir)

    // param "needReplicaEdges" is specialized for v2,
    // as for v1, a replicate process is automatically proceeded while reading edges.
    val needReplicaEdges = params.getOrElse("needReplicaEdges", "false").toBoolean

    // v1 is for normal ego network, it outputs a nodes' neighbors and triangle edges
    // v2 is specialized for large graphs, it outputs a nodes' triangle neighbors and edges
    // results for v2 are saved to hdfs by parquet format
    val version = params.getOrElse("version", "v1")

    start(mode)
    val startTime = System.currentTimeMillis()

    val df = GraphIO.load(input, isWeighted = false, sep = sep)
    var nodeForEgo: RDD[Long] = null
    if (nodePath != null) {
      val nodeForEgoDf = GraphIO.loadNode(nodePath, sep = sepInNodePath)
      nodeForEgo = nodeForEgoDf.select("node").rdd
        .filter(row => !row.anyNull)
        .map { row => row.getLong(0) }
    }

    if (version == "v1") {
      // output nodes' ego, will replicate the input edges automatically
      val ego = new EgoNetwork()
        .setPartitionNum(partitionNum)
        .setStorageLevel(storageLevel)
        .setBatchSize(batchSize)
        .setPullBatchSize(pullBatchSize)
        .setPSPartitionNum(psPartitionNum)
        .setExtraInputs(Array(nodePath))
      if (nodePath != null) ego.setNodeForEgo(nodeForEgo)
      val out = ego.transform(df)
      GraphIO.save(out, output)
    } else {
      // output nodes' triangle edges only, which is more suitable for large graphs
      val ego = new EgoNetworkV2()
        .setPartitionNum(partitionNum)
        .setStorageLevel(storageLevel)
        .setBatchSize(batchSize)
        .setPullBatchSize(pullBatchSize)
        .setPSPartitionNum(psPartitionNum)
        .setExtraInputs(Array(nodePath))
        .setNeedReplicaEdge(needReplicaEdges)
      if (nodePath != null) ego.setNodeForEgo(nodeForEgo)
      val out = ego.transform(df)
      out.write.mode("overwrite").parquet(output)
    }

    println(s"cost ${System.currentTimeMillis() - startTime} ms")
    stop()
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()

    conf.set("spark.hadoop." + AngelConf.ANGEL_PS_BACKUP_AUTO_ENABLE, "false")
    conf.set("spark.hadoop." + AngelConf.ANGEL_PS_JVM_USE_PARALLEL_GC, "true")
    conf.set("spark.hadoop." + AngelConf.ANGEL_PS_JVM_PARALLEL_GC_USE_ADAPTIVE_SIZE, "false")

    conf.setMaster(mode)
    conf.setAppName("egoNetwork")
    val sc = SparkContext.getOrCreate(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}
