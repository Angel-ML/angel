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
package com.tencent.angel.spark.ml.graph.algo.statistics.sssp

import java.util.Collections

import com.tencent.angel.exception.AngelException
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.graph.utils.params.{HasDstNodeIdCol, HasIsWeighted, HasOutputDistanceCol, HasOutputNodeIdCol, HasPSPartitionNum, HasPartitionNum, HasSrcNodeIdCol, HasStorageLevel, HasUseBalancePartition, HasWeightCol}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{LongParam, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{FloatType, LongType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

class SSSP(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputDistanceCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum
  with HasWeightCol with HasIsWeighted with HasUseBalancePartition {

  final val source = new LongParam(this, "source", "source")

  final def setSource(node: Long): this.type = set(source, node)

  def this() = this(Identifiable.randomUID("SSSP"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    var edges: RDD[(Long, Long, Float)] = null
    if ($(isWeighted)) {
      edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol), $(weightCol)).rdd
        .map(row => (row.getLong(0), row.getLong(1), row.getFloat(2)))
        .filter(f => f._1 != f._2)
    } else {
      edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
        .map(row => (row.getLong(0), row.getLong(1), 1.0f))
        .filter(f => f._1 != f._2)
    }

    edges.persist(StorageLevel.DISK_ONLY)

    val index = edges.flatMap(f => Array(f._1, f._2))
    val maxId = edges.flatMap(f => Array(f._1, f._2)).max() + 1
    val minId = edges.flatMap(f => Array(f._1, f._2)).min()
    val numEdges = edges.count()

    println(s"minId=$minId maxId=$maxId numEdges=$numEdges source=${$(source)}")

    val model = SSSPPSModel.fromMinMax(minId, maxId, index, $(psPartitionNum), $(useBalancePartition))
    val graph = edges.map(sd => (sd._1, (sd._2, sd._3))).groupByKey($(partitionNum))
      .mapPartitionsWithIndex((index, it) =>
        Iterator.single(SSSPGraphPartition.apply(index, it)))

    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)
    graph.map(_.initMsgs(model, $(source))).reduce(_ + _)
    model.resetMsgs()

    var numMsgs = model.numMsgs()
    println(s"numMsgs=$numMsgs")
    var curIteration = 0
    do {
      curIteration += 1
      graph.map(_.process(model, numMsgs)).reduce(_ + _)
      model.resetMsgs()
      numMsgs = model.numMsgs()
      println(s"curIteration=$curIteration numMsgs=$numMsgs")
    } while (numMsgs > 0)

    val (partitionIds, ends) = splitPartitionIds(model)
    val retRDD = graph.map(_.save(model, partitionIds, ends)).flatMap(f => f._1.zip(f._2))
      .map { case (node, distance) => Row.fromSeq(Seq[Any](node, distance))}

    dataset.sparkSession.createDataFrame(retRDD, transformSchema(dataset.schema))
  }

  def splitPartitionIds(model: SSSPPSModel): (Array[Int], Array[Int]) = {
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
      StructField(s"${$(outputNodeIdCol)}", LongType, nullable = false),
      StructField(s"${$(outputDistanceCol)}", FloatType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}
