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

package com.tencent.angel.graph.rank.closeness

import java.util.Collections

import com.tencent.angel.graph.utils.io.Log
import com.tencent.angel.graph.utils.params._
import com.tencent.angel.psagent.PSAgentContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{BooleanParam, IntParam, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{FloatType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

/**
  * Implementation of Effective Closeness algorithm proposed by [[http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.231.8735&rep=rep1&type=pdf Kang et al., 2011]].
  * Utilizes HyperLogLog++ counter to approximate cardinality for each node.
  * Note that this algorithm only supports unweighted graph.
  */

class Closeness(override val uid: String) extends Transformer
  with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCentralityCol with HasSrcNodeIdCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasBatchSize with HasMaxIter
  with HasWeightCol with HasIsWeighted with HasUseBalancePartition with HasBalancePartitionPercent {

  /**
    * The value of p and sp define the precision of
    * the Normal and Sparse set representations for HLLCounters.
    * It's recommended to set sp = 0 (i.e. to use normal representation)
    * Note that p must be at least 4, and that
    * when sp != 0, p must be a value between 4 and sp, while sp must be less than 32.
    */
  final val p = new IntParam(this, "p", "p")
  final val sp = new IntParam(this, "sp", "sp")
  // final val maxIter = new IntParam(this, "maxIter", "maxIter")
  final val msgNumBatch = new IntParam(this, "msgBatchSize", "msgBatchSize")
  final val verboseSaving = new BooleanParam(this, "verboseSaving", "verboseSaving")
  final val isDirected = new BooleanParam(this, "isDirected", "isDirected")

  final def setP(precision: Int): this.type = set(p, precision)

  final def setSp(precision: Int): this.type = set(sp, precision)

  final def setMaxIter(iter: Int): this.type = set(maxIter, iter)

  final def setMsgNumBatch(size: Int): this.type = set(msgNumBatch, size)

  final def setVerboseSaving(verbose: Boolean): this.type = set(verboseSaving, verbose)

  final def setIsDirected(directed: Boolean): this.type = set(isDirected, directed)

  setDefault(p, 6)
  setDefault(sp, 0)
  setDefault(maxIter, 10)
  setDefault(msgNumBatch, 4)
  setDefault(verboseSaving, false)
  setDefault(isDirected, true)
  setDefault(balancePartitionPercent, 0.5f)

  def this() = this(Identifiable.randomUID("Closeness"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val edges =
      if ($(isDirected))
        dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
          .filter(row => !row.anyNull)
          .map(row => (row.getLong(0), row.getLong(1)))
          .filter(f => f._1 != f._2)
      else
        dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
          .filter(row => !row.anyNull)
          .flatMap(row => Iterator((row.getLong(0), row.getLong(1)), (row.getLong(1), row.getLong(0))))
          .filter(f => f._1 != f._2)
    edges.persist(StorageLevel.DISK_ONLY)

    val index = edges.flatMap(f => Array(f._1, f._2))
    val (minId, maxId, numEdges) = edges.mapPartitions(summarizeApplyOp).reduce(summarizeReduceOp)

    Log.withTimePrintln(s"minId=$minId maxId=$maxId numEdges=$numEdges p=${$(p)} sp=${$(sp)}")

    val model = ClosenessPSModel.fromMinMax(minId, maxId + 1, index, $(psPartitionNum), $(useBalancePartition), $(balancePartitionPercent))
    val graph = edges.groupByKey($(partitionNum))
      .mapPartitionsWithIndex((index, it) =>
        Iterator.single(ClosenessPartition.apply(index, it, $(p), $(sp))))

    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)

    graph.map(_.init(model)).collect()

    //Loop
    var i = 1
    var numActives = 1L
    do {
      numActives = graph.map(_.process(model, $(msgNumBatch))).reduce(_ + _)
      model.computeCloseness(i)
      Log.withTimePrintln(s"Closeness finished iteration + $i, and the number of active msg is $numActives")
      i += 1
    } while (i <= $(maxIter) && numActives > 0)

    val numNodes = model.numNodes()
    val maxCardinality = model.maxCardinality()
    Log.withTimePrintln(s"numNodes=$numNodes maxCardinality=$maxCardinality")

    val (partitionIds, ends) = splitPartitionIds(model)
    val retRDD = if ($(verboseSaving)) {
      graph.map(_.saveClosenessAndCentrality(model, partitionIds, ends, maxCardinality, $(isDirected)))
        .flatMap(f => f._1.zip(f._2)).map { case (node, res) => Row.fromSeq(Seq[Any](node, res._1.toFloat, res._2, res._3)) }
    } else {
      graph.map(_.save(model, partitionIds, ends, maxCardinality)).flatMap(f => f._1.zip(f._2))
        .map { case (node, rank) => Row.fromSeq(Seq[Any](node, rank)) }
    }

    val outputSchema = schema($(verboseSaving))
    dataset.sparkSession.createDataFrame(retRDD, outputSchema)
  }

  def summarizeApplyOp(iterator: Iterator[(Long, Long)]): Iterator[(Long, Long, Long)] = {
    var minId = Long.MaxValue
    var maxId = Long.MinValue
    var numEdges = 0
    while (iterator.hasNext) {
      val entry = iterator.next()
      val (src, dst) = (entry._1, entry._2)
      if (src <= dst) {
        minId = math.min(minId, src)
        maxId = math.max(maxId, dst)
      } else {
        minId = math.min(minId, dst)
        maxId = math.max(maxId, src)
      }
      numEdges += 1
    }

    Iterator.single((minId, maxId, numEdges))
  }

  def summarizeReduceOp(t1: (Long, Long, Long),
                        t2: (Long, Long, Long)): (Long, Long, Long) =
    (math.min(t1._1, t2._1), math.max(t1._2, t2._2), t1._3 + t2._3)

  def splitPartitionIds(model: ClosenessPSModel): (Array[Int], Array[Int]) = {
    val parts = PSAgentContext.get().getMatrixMetaManager.getPartitions(model.matrixId)
    Collections.shuffle(parts)

    val length = parts.size()
    val sizes = new Array[Int]($(partitionNum))
    for (i <- sizes.indices)
      sizes(i) = length / sizes.length
    for (i <- 0 until (length % sizes.length))
      sizes(i) += 1

    for (i <- 1 until sizes.length)
      sizes(i) += sizes(i - 1)

    val partitionIds = new Array[Int](length)
    for (i <- 0 until length)
      partitionIds(i) = parts.get(i).getPartitionId

    (partitionIds, sizes)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"$outputNodeIdCol", LongType, nullable = false),
      StructField(s"$outputCentralityCol", FloatType, nullable = false)
    ))
  }

  def schema(verbose: Boolean): StructType = {
    if (verbose)
      StructType(Seq(
        StructField(s"$outputNodeIdCol", LongType, nullable = false),
        StructField(s"$outputCentralityCol", FloatType, nullable = false),
        StructField(s"cardinality", LongType, nullable = false),
        StructField(s"distSum", LongType, nullable = false)
      ))
    else
      StructType(Seq(
        StructField(s"$outputNodeIdCol", LongType, nullable = false),
        StructField(s"$outputCentralityCol", FloatType, nullable = false)
      ))
  }


  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}
