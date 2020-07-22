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

package com.tencent.angel.graph.rank.hindex

import com.tencent.angel.graph.utils.io.Log
import com.tencent.angel.graph.utils.params._
import com.tencent.angel.spark.context.PSContext
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

class HIndex(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize
  with HasBufferSize with HasUseBalancePartition {

  def this() = this(Identifiable.randomUID("HIndex"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    // read edges
    val edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(e => e._1 != e._2)
    // persist edges using Disk_Only
    edges.persist(StorageLevel.DISK_ONLY)

    val nodes = edges.flatMap(e => Iterator(e._1, e._2))
    val maxId = nodes.max() + 1
    val minId = nodes.min()
    val numEdges = edges.count()

    Log.withTimePrintln(s"minId=$minId maxId=$maxId numEdges=$numEdges storageLevel=${$(storageLevel)}")

    // start PS
    Log.withTimePrintln("start to run ps")
    val beforeStartPS = System.currentTimeMillis()
    PSContext.getOrCreate(SparkContext.getOrCreate())
    Log.withTimePrintln(s"Starting ps cost ${System.currentTimeMillis() - beforeStartPS} ms")

    // init the model
    val model = HIndexPSModel.fromMinMax(minId, maxId, nodes, $(psPartitionNum), $(useBalancePartition))

    // create sub-graph partitions
    val graph = edges.flatMap { case (srcId, dstId) => Iterator((srcId, dstId), (dstId, srcId)) }
      .groupByKey($(partitionNum))
      .mapPartitionsWithIndex((index, adjTable) => Iterator(HIndexPartition.apply(index, adjTable)))
    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)

    //init msgs with each node's degree
    graph.foreach(_.initMsgs(model))

    // compute hindex, windex and gindex for each node
    val res = graph.map(_.process(model))
    res.persist()
    res.count()

    val retRDD = res.flatMap { case (node, cores) => node.zip(cores) }
    val result = retRDD.map(r => Row.fromSeq(Seq[Any](r._1, r._2._1, r._2._2, r._2._3)))

    dataset.sparkSession.createDataFrame(result, transformSchema(dataset.schema))

  }


  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"$outputNodeIdCol", LongType, nullable = false),
      StructField(s"h-index", IntegerType, nullable = false),
      StructField(s"g-index", IntegerType, nullable = false),
      StructField(s"w-index", IntegerType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}