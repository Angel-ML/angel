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
import com.tencent.angel.spark.ml.graph.algo.rank.toprelation.TopCandidate
import com.tencent.angel.spark.ml.graph.utils.GraphIO
import com.tencent.angel.spark.ml.util.DataStatistics
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object TopCandidateExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val input = params.getOrElse("input", null)
    val output = params.getOrElse("output",null)
    val partitionNum = params.getOrElse("partitionNum","1").toInt
    val srcIndex = params.getOrElse("src", "0").toInt
    val dstIndex = params.getOrElse("dst", "1").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "DISK_ONLY"))
    val batchSize = params.getOrElse("batchSize", "1000").toInt
    val psPartitionNum = params.getOrElse("psPartitionNum", "1").toInt
    val useBalancePartition = params.getOrElse("useBalancePartition", "false").toBoolean
    val k = params.getOrElse("topk", "5").toInt
    val sep = params.getOrElse("sep", "tab") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }
    val isUnDirected = params.getOrElse("isUnDirected", "false").toBoolean
    // is graph weighted or not
    val isWeighted = params.getOrElse("isWeighted", "false").toBoolean
    val weightIndex = params.getOrElse("weightIndex", "2").toInt

    // is attrs weighted or not
    val isAttrWeighted = params.getOrElse("isAttrWeighted", "true").toBoolean
    val attrWeightIndex = params.getOrElse("attrWeightIndex", "2").toInt

    // params for node attrs files
    val nodeAttrPath = params.getOrElse("nodeAttrPath", null)
    val nodeIdInAttrPath = params.getOrElse("nodeIdInAttrPath", "0").toInt
    // node attrs should be arranged in one column with a separation to distinguish each value
    val attrIndex = params.getOrElse("attrIndexCols", "1").toInt
    // separation between columns in node attr files
    val sepInAttrs = params.getOrElse("sepInAttrs", "tab") match {
      case "space" => " "
      case "comma" => ","
      case "tab" => "\t"
    }
    //    assert(sepInAttrs != sep, s"separation of node column and attrs column should be different from separation of attrs.")

    val sc = start(mode)
    val cpDir = params.get("cpDir").filter(_.nonEmpty).orElse(GraphIO.defaultCheckpointDir)
      .getOrElse(throw new Exception("checkpoint dir not provided"))
    sc.setCheckpointDir(cpDir)

    val df = GraphIO.load(input, isWeighted = isWeighted, srcIndex, dstIndex, weightIndex, sep = sep)
    val minNode = df.agg(Map("src" -> "min", "dst" -> "min")).collect()
    val maxNode = df.agg(Map("src" -> "max", "dst" -> "max")).collect()
    val numEdges = df.agg(Map("src" -> "count")).collect()

    val minId = math.min(minNode(0).getLong(0), minNode(0).getLong(1))
    val maxId = math.max(maxNode(0).getLong(0), maxNode(0).getLong(1))

    println(s"minId: $minId, maxId: $maxId, num of edges: ${numEdges(0).getLong(0)}")

    val topCandidate = new TopCandidate()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setNodeAttrPath(nodeAttrPath)
      .setBatchSize(batchSize)
      .setUseBalancePartition(useBalancePartition)
      .setTopK(k)
      .setIsWeighted(isWeighted)
      .setUnDirected(isUnDirected)
      .setMinNodeId(minId)
      .setMaxNodeId(maxId)

    // read and set nodeAttr RDD
    topCandidate.setNodeAttr(readAttrs(nodeAttrPath, nodeIdInAttrPath, attrIndex, isAttrWeighted, attrWeightIndex: Int, sepInAttrs: String, partitionNum))

    //collect app name and data size
    DataStatistics.setAppName(topCandidate.getClass.getCanonicalName)
    DataStatistics.calcRDDSize(df, "angel.input.data")
    DataStatistics.setDataPartitionNum(partitionNum)
    DataStatistics.setPSPartitionNum(psPartitionNum)

    val mapping = topCandidate.transform(df)
    GraphIO.save(mapping, output)
    stop()
  }

  def readAttrs(path: String, nodeIndex: Int, attrIndex: Int, isWeighted: Boolean, attrWeightIndex: Int, sepOfNodeAttr: String, partitionNum: Int): RDD[(Long, Array[Float])] = {
    val rawAttrs = GraphIO.load(path, isWeighted, nodeIndex, attrIndex, attrWeightIndex, sep = sepOfNodeAttr)

    val attrs = if (isWeighted) {
      rawAttrs.select("src", "dst", "weight").rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getLong(0), (row.getLong(1).toFloat, row.getFloat(2))))
        .filter(f => f._2._2 != 0)
    } else {
      rawAttrs.select("src", "dst").rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getLong(0), (row.getLong(1).toFloat, 1.0f)))
    }
    attrs.groupByKey(partitionNum).map(x => (x._1, x._2.toArray.flatMap(y => Iterator(y._1, y._2))))
  }

  def start(mode: String = "local"): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("topCandidates")
    conf.set(AngelConf.ANGEL_PSAGENT_UPDATE_SPLIT_ADAPTION_ENABLE, "false")
    val sc = new SparkContext(conf)
    //    sc.setLogLevel("WARN")
    //    sc.setCheckpointDir("cp")
    //    PSContext.getOrCreate(sc)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }
}
