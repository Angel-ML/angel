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

package com.tencent.angel.graph.connectedcomponent.wcc

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


class WCC(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasOutputNodeIdCol with HasOutputCoreIdCol with HasBalancePartitionPercent
  with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasUseBalancePartition {

  def this() = this(Identifiable.randomUID("WCC"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    // read edges
    val edges = dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
      .filter(row => !row.anyNull)
      .map(row => (row.getLong(0), row.getLong(1)))
      .filter(e => e._1 != e._2)

    edges.persist(StorageLevel.DISK_ONLY)

    val nodes = edges.flatMap(e => Iterator(e._1, e._2))
    val maxId = nodes.max() + 1
    val minId = nodes.min()
    val numEdges = edges.count()

    Log.withTimePrintln(s"minId=$minId maxId=$maxId numEdges=$numEdges storageLevel=${$(storageLevel)}")

    // Start PS and init the model
    Log.withTimePrintln("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())

    val model = WCCPSModel.fromMinMax(minId, maxId, nodes, $(psPartitionNum), $(useBalancePartition), $(balancePartitionPercent))

    // make un-directed graph, for wcc
    var graph = edges.flatMap { case (srcId, dstId) => Iterator((srcId, dstId), (dstId, srcId)) }
      .groupByKey($(partitionNum))
      .mapPartitionsWithIndex((index, adjTable) => Iterator(WCCPartition.apply(index, adjTable)))

    graph.persist($(storageLevel))
    graph.foreachPartition(_ => Unit)

    //Loop
    var numChanged = graph.map(_._1.initMsgs(model)).reduce(_ + _)
    var i = 0
    var prev = graph
    Log.withTimePrintln(s"init wcc labels to all vertices ")
    // each node change its label into the min id of its neighbors (including itself).

    do {
      i += 1
      graph = prev.map(_._1.process(model, numChanged, i == 1))
      graph.persist($(storageLevel))
      numChanged = graph.map(_._2).reduce(_ + _)
      graph.count()
      prev.unpersist(true)
      prev = graph
      model.resetMsgs()

      Log.withTimePrintln(s"WCC finished iteration + $i, and $numChanged nodes changed  wcc label")
    } while (numChanged > 0)

    val retRDD = graph.map(_._1.save()).flatMap(f => f._1.zip(f._2))
      .map(f => Row.fromSeq(Seq[Any](f._1, f._2)))

    dataset.sparkSession.createDataFrame(retRDD, transformSchema(dataset.schema))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"$outputNodeIdCol", LongType, nullable = false),
      StructField(s"$outputCoreIdCol", LongType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}
