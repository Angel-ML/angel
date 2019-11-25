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

package com.tencent.angel.spark.ml.graph.clusterrank

import com.tencent.angel.spark.ml.graph.data.VertexId
import com.tencent.angel.spark.ml.graph.params._
import com.tencent.angel.spark.ml.graph.{GraphOps, NeighborTableModel, OutDegreeModel}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}


class ClusterRankDirected(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize
  with HasBufferSize with HasCommonFriendsNumCol with HasDebugMode
  with HasSrcNodeIndex with HasDstNodeIndex
  with HasInput with HasExtraInputs with HasDelimiter with HasOutputTriangleCol
  with HasSrcNodeLccCol {

  def this() = this(Identifiable.randomUID("clusterrank_directed"))

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

    println(s"3. convert edges to neighbor table")
    val neighborRDD = GraphOps.edgesToNeighborTable(edges, $(partitionNum))

    println(s"4. build neighbor table partition")
    val neighborPartitions = GraphOps.buildNeighborTablePartition(neighborRDD)
    neighborPartitions.persist($(storageLevel))

    println(s"5. calculate stats of graph")
    val stats = GraphOps.getStats(neighborPartitions)
    println(stats)

    println(s"6. start parameter server")
    val psStartTime = System.currentTimeMillis()
    NeighborTableModel.startPS(dataset.sparkSession.sparkContext)
    println(s"start parameter server costs ${System.currentTimeMillis() - psStartTime} ms")

    println(s"7. push neighbor tables to parameter server")
    val neighborModel = NeighborTableModel(stats.maxVertexId + 1, $(batchSize), $(pullBatchSize), $(psPartitionNum))
    var pushStartTs = System.currentTimeMillis()
    neighborModel.initLongNeighbor(neighborPartitions)
    println(s"push costs ${System.currentTimeMillis() - pushStartTs} ms")


    println(s" build degree RDD")
    val degreeRdd = GraphOps.buildOutDegreeTable(edges, $(partitionNum))

    val outDegreeModel = OutDegreeModel.create(stats.maxVertexId + 1, $(batchSize), $(pullBatchSize), $(psPartitionNum))
    println(s" push out-degree table to parameter server")
    println(s"======sampled degreeRdd======")
    degreeRdd.take(20).foreach(f =>
      println(s"${f._1} : ${f._2}")
    )

    pushStartTs = System.currentTimeMillis()
    outDegreeModel.init(degreeRdd)
    println(s"push outDegree costs ${System.currentTimeMillis() - pushStartTs} ms")

    println(s" calculate number of edges in neighbor")
    val numEdgeRdd: RDD[(VertexId, Long)] =
      neighborModel.calNumEdgesInOutNeighbor(neighborPartitions).persist($(storageLevel))

    println(s"======sampled numEdgeRdd======")
    numEdgeRdd.take(20).foreach{ case(node, tc) =>
      println(s"$node : $tc")
    }

    val totalNumEdges: Long = numEdgeRdd.map(_._2).reduce(_ + _)
    println(s"==> total num of edges in neighbor = $totalNumEdges")

    println(s" push number of edges in neighbor to parameter server")
    val neighborEdgesModel = NeighborEdgesModel.create(stats.maxVertexId + 1, $(batchSize), $(pullBatchSize), $(psPartitionNum))
    pushStartTs = System.currentTimeMillis()
    neighborEdgesModel.init(numEdgeRdd)
    println(s"push neighborEdges costs ${System.currentTimeMillis() - pushStartTs} ms")

    val rankRdd = neighborModel.calClusterRank(neighborPartitions, outDegreeModel, neighborEdgesModel)

    println(s"======sampled output======")
    rankRdd.take(20).foreach { r => println(r.getLong(0) +":" + " rank: " + r.getDouble(1)) }

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(rankRdd, outputSchema)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField($(srcNodeIdCol), LongType, nullable = false),
      StructField($(srcNodeLccCol), DoubleType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}



