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
package com.tencent.angel.spark.ml.graph.algo.intimacy.linkprediction

import com.tencent.angel.spark.ml.graph
import com.tencent.angel.spark.ml.graph.algo.statistics.commonfriends.CommonFriendsPSModel
import com.tencent.angel.spark.ml.graph.data.{NeighborTable, NeighborTablePartition}
import com.tencent.angel.spark.ml.graph.{GraphOps, NeighborTableModel, NeighborTableOps}
import com.tencent.angel.spark.ml.graph.utils.params._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class LinkPrediction(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize
  with HasBufferSize with HasCommonFriendsNumCol with HasDebugMode
  with HasSrcNodeIndex with HasDstNodeIndex
  with HasInput with HasExtraInputs with HasDelimiter {

  def this() = this(Identifiable.randomUID("link_prediction"))

  override def transform(dataset: Dataset[_]): DataFrame = {

    val sc = dataset.sparkSession.sparkContext
    assert(sc.getCheckpointDir.nonEmpty, "set checkpoint dir first")
    println(s"partition number: ${$(partitionNum)}")

    println("1. load edges from the dataset")
    val edges = GraphOps.loadEdges(dataset,
      $(srcNodeIdCol),
      $(dstNodeIdCol))
    println(s"sample edges = ")
    println(edges.take(10).mkString(","))

    println(s"2. convert edges to neighbor table")
    val neighborRDD = GraphOps.edgesToNeighborTable(edges, $(partitionNum))
    println(s"sample neighbor table = ")
    neighborRDD.take(10).foreach(println)

    println(s"3. build neighbor table partition")
    val neighborPartitions = GraphOps.buildNeighborTablePartition(neighborRDD)

    println(s"4: calculate stats of graph")
    val stats = GraphOps.getStats(neighborPartitions)
    println(stats)

    println(s"5：start parameter server")
    val psStartTime = System.currentTimeMillis()
    NeighborTableModel.startPS(dataset.sparkSession.sparkContext)
    println(s"start parameter server costs ${System.currentTimeMillis() - psStartTime} ms")

    println(s"6：push neighbor tables to parameter server")
    val initTableStartTime = System.currentTimeMillis()
    val psModel = NeighborTableModel(stats.maxVertexId + 1, $(batchSize), $(pullBatchSize), $(psPartitionNum))
    psModel.initLongNeighbor(neighborPartitions)
    psModel.testPS(neighborPartitions)
    println(s"push costs ${System.currentTimeMillis() - initTableStartTime} ms")
    val cpTableStartTime = System.currentTimeMillis()
    println(s"7. checkpoint of neighbor table")
    psModel.checkpoint()
    println(s"checkpoint costs ${System.currentTimeMillis() - cpTableStartTime} ms")

    println(s"8. calculate link prediction metrics")
    val output = psModel.calLinkPrediction(neighborPartitions).map{ edge =>
      Row(edge.srcId,
        edge.dstId,
        edge.attr.commonFriends,
        edge.attr.totalFriends,
        edge.attr.preferAttachment,
        edge.attr.adamicAdar,
        edge.attr.resourceAllocation)
    }

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(output, outputSchema)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField($(srcNodeIdCol), LongType, nullable = false),
      StructField($(dstNodeIdCol), LongType, nullable = false),
      StructField("common_friends", LongType, nullable = false),
      StructField("total_friends", LongType, nullable = false),
      StructField("preferential_attachment", LongType, nullable = false),
      StructField("adamic_adar", DoubleType, nullable = false),
      StructField("resource_allocation", DoubleType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}
