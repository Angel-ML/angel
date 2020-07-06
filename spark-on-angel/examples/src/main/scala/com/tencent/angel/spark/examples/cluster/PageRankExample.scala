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
import com.tencent.angel.graph.rank.pagerank.edgecut.{PageRank => EdgeCutPageRank}
import com.tencent.angel.graph.rank.pagerank.vertexcut.{PageRank => VertexCutPageRank}
import com.tencent.angel.graph.utils.{Delimiter, GraphIO}
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object PageRankExample {

  def main(args: Array[String]): Unit = {
    val params = ArgsUtil.parse(args)
    val mode = params.getOrElse("mode", "yarn-cluster")
    val sc = start(mode)

    val input = params.getOrElse("input", "")
    val partitionNum = params.getOrElse("dataPartitionNum", "100").toInt
    val storageLevel = StorageLevel.fromString(params.getOrElse("storageLevel", "MEMORY_ONLY"))
    val output = params.getOrElse("output", "")
    val psPartitionNum = params.getOrElse("psPartitionNum",
      sc.getConf.get("spark.ps.instances", "10")).toInt
    val tol = params.getOrElse("tol", "0.01").toFloat
    val resetProp = params.getOrElse("resetProp", "0.15").toFloat
    val isWeight = params.getOrElse("isWeight", "false").toBoolean
    val srcIndex = params.getOrElse("srcIndex", "0").toInt
    val dstIndex = params.getOrElse("dstIndex", "1").toInt
    val weightIndex = params.getOrElse("weightIndex", "2").toInt
    val useBalancePartition = params.getOrElse("useBalancePartition", "false").toBoolean
    val balancePartitionPercent = params.getOrElse("balancePartitionPercent", "0.7").toFloat
    val version = params.getOrElse("version", "edge-cut")
    val numBatch = params.getOrElse("numBatch", "1").toInt
    val sep = Delimiter.parse(params.getOrElse("sep",Delimiter.SPACE))


    val edges = GraphIO.load(input, isWeighted = isWeight,
      srcIndex = srcIndex, dstIndex = dstIndex,
      weightIndex = weightIndex, sep = sep)

    val ranks = version match {
      case "edge-cut" => edgeCutPageRank(edges, partitionNum, psPartitionNum,
        storageLevel, tol, resetProp, isWeight,
        useBalancePartition, balancePartitionPercent, numBatch)
      case "vertex-cut" => vertexCutPageRank(edges, partitionNum, psPartitionNum,
        storageLevel, tol, resetProp, isWeight,
        useBalancePartition, balancePartitionPercent, numBatch)
    }

    GraphIO.save(ranks, output)
    stop()
  }

  def vertexCutPageRank(edges: DataFrame, partitionNum: Int,
                        psPartitionNum: Int,
                        storageLevel: StorageLevel,
                        tol: Float, resetProb: Float, isWeighted: Boolean,
                        useBalancePartition: Boolean,
                        balancePartitionPercent: Float, numBatch: Int): DataFrame = {
    val pageRank = new VertexCutPageRank()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setTol(tol)
      .setResetProb(resetProb)
      .setIsWeighted(isWeighted)
      .setUseBalancePartition(useBalancePartition)
      .setBalancePartitionPercent(balancePartitionPercent)
      .setNumBatch(numBatch)
    pageRank.transform(edges)
  }

  def edgeCutPageRank(edges: DataFrame, partitionNum: Int,
                      psPartitionNum: Int,
                      storageLevel: StorageLevel,
                      tol: Float, resetProb: Float, isWeighted: Boolean,
                      useBalancePartition: Boolean,
                      balancePartitionPercent: Float, numBatch: Int): DataFrame = {
    val pageRank = new EdgeCutPageRank()
      .setPartitionNum(partitionNum)
      .setStorageLevel(storageLevel)
      .setPSPartitionNum(psPartitionNum)
      .setTol(tol)
      .setResetProb(resetProb)
      .setIsWeighted(isWeighted)
      .setUseBalancePartition(useBalancePartition)
      .setBalancePartitionPercent(balancePartitionPercent)
      .setNumBatch(numBatch)
    pageRank.transform(edges)
  }

  def start(mode: String): SparkContext = {
    val conf = new SparkConf()
    conf.setMaster(mode)
    conf.setAppName("PageRank")
    val sc = new SparkContext(conf)
    sc
  }

  def stop(): Unit = {
    PSContext.stop()
    SparkContext.getOrCreate().stop()
  }

}
