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

package com.tencent.angel.graph.rank.pagerank.edgecut

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.graph.rank.pagerank.PageRankOps
import com.tencent.angel.graph.utils.io.Log
import com.tencent.angel.graph.utils.params._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{BooleanParam, FloatParam, IntParam, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{FloatType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

class PageRank(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputPageRankCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum
  with HasWeightCol with HasIsWeighted with HasUseBalancePartition
  with HasUseEstimatePartition with HasBalancePartitionPercent {


  final val tol = new FloatParam(this, "tol", "tol")
  final val resetProb = new FloatParam(this, "resetProb", "resetProb")
  final val isLeakInserted = new BooleanParam(this, "isLeakInserted", "isLeakInserted")
  final val numBatch = new IntParam(this, "numBatch", "numBatch")

  final def setTol(error: Float): this.type = set(tol, error)

  final def setResetProb(prob: Float): this.type = set(resetProb, prob)

  final def setIsLeakInserted(isLeakInsert: Boolean): this.type = set(isLeakInserted, isLeakInsert)

  final def setNumBatch(batch: Int): this.type = set(numBatch, batch)

  setDefault(tol, 0.01f)
  setDefault(resetProb, 0.15f)
  setDefault(isLeakInserted, false)
  setDefault(numBatch, 1)

  def this() = this(Identifiable.randomUID("PageRank"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val edges = if ($(isWeighted)) {
      dataset.select($(srcNodeIdCol), $(dstNodeIdCol), $(weightCol)).rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getLong(0), row.getLong(1), row.getFloat(2)))
        .filter(f => f._1 != f._2 && f._3 != 0)
    } else {
      dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getLong(0), row.getLong(1), 1.0f))
        .filter(f => f._1 != f._2)
    }
    edges.persist(StorageLevel.DISK_ONLY)

    val index = edges.flatMap(f => Array(f._1, f._2))
    val (minId, maxId, numEdges) = edges.mapPartitions(PageRankOps.summarizeApplyOp)
      .reduce(PageRankOps.summarizeReduceOp)

    val initRank = $(resetProb)

    Log.withTimePrintln(s"minId=$minId maxId=$maxId numEdges=$numEdges tol=${$(tol)}")

    // Start PS and init the model
    Log.withTimePrintln("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    // Create matrix
    val model = PageRankPSModel.fromMinMax(minId, maxId + 1, index,
      $(psPartitionNum), $(useBalancePartition), $(useEstimatePartition), $(balancePartitionPercent))

    val graph = edges.map(sd => (sd._1, (sd._2, sd._3)))
      .groupByKey($(partitionNum))
      .mapPartitionsWithIndex((index, it) => Iterator.single(PageRankPartition.apply(index, it)))

    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)
    graph.map(_.start(model, initRank, $(resetProb), $(tol))).reduce(_ + _)

    model.computeRanks(initRank, $(resetProb))

    val nodesWithoutInLinks = graph.map(_.setMissRanks(model, initRank)).reduce(_ + _)
    val numNodes = model.numNodes()

    Log.withTimePrintln(s"There are $nodesWithoutInLinks nodes without in-degrees")
    Log.withTimePrintln(s"There are $numNodes nodes in total")

    //Loop
    var numMsgs = model.numMsgs()
    var i = 1
    Log.withTimePrintln(s"numMsgs=$numMsgs")

    do {
      graph.map(_.process(model, $(resetProb), $(tol), numMsgs)).reduce(_ + _)
      model.computeRanks(initRank, $(resetProb))
      numMsgs = model.numMsgs()
      Log.withTimePrintln(s"PageRank finished iteration + $i, and the number of msg is $numMsgs")
      i += 1
    } while (numMsgs > 0)

    model.normalizeRanks(numNodes)

    val (partitionIds, ends) = PageRankOps.splitPartitionIds(model.matrixId, graph.getNumPartitions)
    val retRDD = graph.flatMap(f => PageRankOps.save(f.getIndex, model, partitionIds, ends, $(numBatch)))
      .flatMap(f => f._1.zip(f._2))
      .map { case (node, rank) => Row.fromSeq(Seq[Any](node, rank)) }

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(retRDD, outputSchema)
  }


  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"$outputNodeIdCol", LongType, nullable = false),
      StructField(s"$outputPageRankCol", FloatType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}
