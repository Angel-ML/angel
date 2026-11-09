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
package com.tencent.angel.spark.ml.graph.algo.rank.pagerank.pagerankpro

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.algo.rank.pagerank.PageRankOps
import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{BooleanParam, FloatParam, IntParam, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{FloatType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

// a multi-label propagation version, implemented by edge-cut version of pageRank

class PageRankPro(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputPageRankCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum
  with HasWeightCol with HasIsWeighted with HasUseBalancePartition
  with HasUseEstimatePartition with HasBalancePartitionPercent with HasMaxIteration
  with HasNeedReplicaEdge with HasBatchSize {


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

  var label_pos: RDD[(Long, Float)] = _
  var label_neg: RDD[(Long, Float)] = _
  var equalPropagation: Boolean = false

  def setPosLabelRDD(in: RDD[(Long, Float)]): Unit = { this.label_pos = in }
  def setNegLabelRDD(in: RDD[(Long, Float)]): Unit = { this.label_neg = in }
  def setEqualPropagation(in: Boolean): Unit = { this.equalPropagation = in }

  def this() = this(Identifiable.randomUID("PageRankPro"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val edges = if ($(isWeighted)) {
      dataset.select($(srcNodeIdCol), $(dstNodeIdCol), $(weightCol)).rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getLong(0), row.getLong(1), row.getFloat(2)))
        .filter(f => f._1 != f._2)
        .filter(f => f._3 != 0)
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

    println(s"minId=$minId maxId=$maxId numEdges=$numEdges tol=${$(tol)}")

    // Start PS and init the model
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    // Create matrix
    val model = PageRankProPSModel.fromMinMax(minId, maxId + 1, index,
      $(psPartitionNum), $(useBalancePartition), $(useEstimatePartition), $(balancePartitionPercent))
    val graph = PageRankPro.makeGraph(edges, $(partitionNum), $(needReplicaEdge))

    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)

    // (seedNode, rank, degree), equal risk propagation
    val init_ = if (equalPropagation) {
      label_pos.filter(x => x._1 >= minId && x._1 <= maxId)
        .filter(_._2 > $ {tol}).leftOuterJoin(graph.flatMap(_.getDegree()))
        .map(x => (x._1, x._2._1, x._2._2.getOrElse(0)))
        .filter(_._3 > 0)
        .map(x => (x._1, x._2 * x._3))
    } else {
      label_pos.filter(x => x._1 >= minId && x._1 <= maxId).filter(_._2 > $ {tol})
    }

    println(s"numLabels=${label_pos.count()} numValidLabels=${init_.count()}")
    model.initLabelRank(init_)
    val minRank = init_.map(_._2).min()
    val tolerance = math.min($(tol), minRank)
    println(s"minRank=$minRank setTol=${${tol}} tolerance=$tolerance")
    graph.map(_.start(model, initRank, $(resetProb), tolerance)).reduce(_ + _)
    println(s"num write msgs: ${model.numWriteMsgs()}")

    model.computeRanks3($(resetProb))

    var numMsgs = model.numMsgs()
    var numIterations = 1
    println(s"numMsgs=$numMsgs rankSum=${model.rankSum()}")

    do {
      graph.map(_.process(model, $(resetProb), tolerance, numMsgs)).reduce(_ + _)
      model.computeRanks3($(resetProb))
      numMsgs = model.numMsgs()
      println(s"numIteration=$numIterations numMsgs=$numMsgs")
      numIterations += 1
    } while (numMsgs > 0 && numIterations <= ${maxIteration} )
    // && numIterations <= ${maxIteration}

    val numNodes = model.numNodes()
    model.normalizeRanks(numNodes)


    val batchs = math.max($(psPartitionNum) / $(numBatch), $(batchSize))
    val retRDD = edges.flatMap(f => Array(f._1, f._2))
      .map(key=> (key, 1))
      .groupByKey($(partitionNum))
      .mapPartitions{ iter =>
        val batchIterator = iter.map(_._1).sliding(batchs, batchs)

        new Iterator[Array[(Long, Float)]] with Serializable {
          override def hasNext: Boolean = {
            batchIterator.hasNext
          }

          override def next: Array[(Long, Float)] = {
            val batch = batchIterator.next().toArray
            val ranks = model.readRanks(batch)
            batch.map(key => (key, ranks.get(key)))
          }
        }
      }.flatMap(p=>p)
      .map { case (node, rank) => Row.fromSeq(Seq[Any](node, rank))}

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(retRDD, outputSchema)
  }



  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"${$(outputNodeIdCol)}", LongType, nullable = false),
      StructField(s"${$(outputPageRankCol)}", FloatType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

object PageRankPro {
  def makeGraph(edges: RDD[(Long, Long, Float)], numPartitions: Int, needReplica: Boolean = false) = {
    if (needReplica) {
      edges.flatMap(sd => Iterator((sd._1, (sd._2, sd._3)), (sd._2, (sd._1, sd._3)))).groupByKey(numPartitions)
        .mapPartitionsWithIndex((index, it) =>
          Iterator.single(PageRankProGraphPartition.apply(index, it)))
    } else {
      edges.map(sd => (sd._1, (sd._2, sd._3))).groupByKey(numPartitions)
        .mapPartitionsWithIndex((index, it) =>
          Iterator.single(PageRankProGraphPartition.apply(index, it)))
    }
  }
}
