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

package com.tencent.angel.graph.statistics.triangle

import com.tencent.angel.graph.utils.io.Log
import com.tencent.angel.graph.utils.params._
import com.tencent.angel.graph.{GraphOps, NeighborTableModel}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class TriangleCountingUndirected(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize with HasOutputTriangleCol
  with HasSrcNodeLccCol with HasComputeLcc {

  def this() = this(Identifiable.randomUID("TriangleCountingUndirected"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    // load edges
    val edges = GraphOps.loadEdges(dataset, $(srcNodeIdCol), $(dstNodeIdCol))
      .flatMap(f => Iterator((f._1, f._2), (f._2, f._1)))

    // convert edges to neighbor table
    val neighborRDD = GraphOps.edgesToNeighborTable(edges, $(partitionNum))

    // build neighbor table partition
    val neighborPartitions = GraphOps.buildNeighborTablePartition(neighborRDD)
    neighborPartitions.persist($(storageLevel))

    // calculate stats of graph
    val stats = GraphOps.getStats(neighborPartitions)
    Log.withTimePrintln(stats.toString)

    // start parameter server
    val psStartTime = System.currentTimeMillis()
    NeighborTableModel.startPS(dataset.sparkSession.sparkContext)
    Log.withTimePrintln(s"starting ps cost ${System.currentTimeMillis() - psStartTime} ms")

    // push neighbor table to parameter server
    val initTableStartTime = System.currentTimeMillis()
    val neighborModel = NeighborTableModel(stats.maxVertexId + 1,
      $(batchSize), $(pullBatchSize), $(psPartitionNum))

    neighborModel.initLongNeighbor(neighborPartitions)
    Log.withTimePrintln(s"pushing neighbor table to ps cost ${System.currentTimeMillis() - initTableStartTime} ms")

    // triangle counting
    val resRdd = neighborModel.calTriangleUndirected(neighborPartitions, $(computeLcc)).persist($(storageLevel))

    val res = if ($(computeLcc)) {
      dataset.sparkSession.createDataFrame(resRdd.map(r => Row(r._1, r._2, r._3)),
        transformSchemaWithLCC(dataset.schema))
    } else {
      dataset.sparkSession.createDataFrame(resRdd.map(r => Row(r._1, r._2)),
        transformSchema(dataset.schema))
    }
    res
  }

  def transformSchemaWithLCC(schema: StructType): StructType = {
    StructType(Seq(
      StructField($(srcNodeIdCol), LongType, nullable = false),
      StructField($(outputTriangleCol), IntegerType, nullable = false),
      StructField($(srcNodeLccCol), FloatType, nullable = false)
    ))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField(s"$srcNodeIdCol", LongType, nullable = false),
      StructField(s"$outputTriangleCol", IntegerType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}
