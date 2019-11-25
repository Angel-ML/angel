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

package com.tencent.angel.spark.ml.graph.triangle

import com.tencent.angel.spark.ml.graph.data.{CounterTriangleDirected, VertexId}
import com.tencent.angel.spark.ml.graph.params._
import com.tencent.angel.spark.ml.graph.{GraphOps, NeighborTableModel}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel


class LCCDirected(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize
  with HasBufferSize with HasCommonFriendsNumCol with HasDebugMode
  with HasSrcNodeIndex with HasDstNodeIndex
  with HasInput with HasExtraInputs with HasDelimiter with HasOutputTriangleCol
  with HasSrcNodeLccCol {

  def this() = this(Identifiable.randomUID("lcc_directed"))

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

    println(s"2. build degree RDD")
    val degreeRdd = GraphOps.buildVertexDegreeTable(edges, $(partitionNum)).persist(StorageLevel.DISK_ONLY)

    println(s"3. convert edges to neighbor table")
    val neighborRDD = GraphOps.edgesToNeighborTableWithByteTags(edges, $(partitionNum))
//    println(s"sample neighbor table = ")
//    neighborRDD.take(10).foreach(println)

    println(s"4. build neighbor table partition")
    val neighborPartitions = GraphOps.buildNeighborTablePartition(neighborRDD)
    neighborPartitions.persist($(storageLevel))

    println(s"5: calculate stats of graph")
    val stats = GraphOps.getStats(neighborPartitions)
    println(stats)

    println(s"6：start parameter server")
    val psStartTime = System.currentTimeMillis()
    NeighborTableModel.startPS(dataset.sparkSession.sparkContext)
    println(s"start parameter server costs ${System.currentTimeMillis() - psStartTime} ms")

    println(s"7：push neighbor tables to parameter server")
    val initTableStartTime = System.currentTimeMillis()
    val neighborModel = NeighborTableModel(stats.maxVertexId + 1, $(batchSize), $(pullBatchSize), $(psPartitionNum))

    neighborModel.initLongNeighborByteAttr(neighborPartitions)
    println(s"push costs ${System.currentTimeMillis() - initTableStartTime} ms")
//    val cpTableStartTime = System.currentTimeMillis()
//    println(s"7. checkpoint of neighbor table")
//    neighborModel.checkpoint()
//    println(s"checkpoint costs ${System.currentTimeMillis() - cpTableStartTime} ms")

    println(s"8. calculate number of edges in neighbors")
    val resRdd: RDD[(VertexId, Long, Seq[(VertexId, Long)])]
      = neighborModel.calNumEdgesInNeighbor(neighborPartitions).persist($(storageLevel))

    val totalTriangles: Long = resRdd.map(row => row._2).reduce(_ + _)
    println(s"==> total num of triangles directed = $totalTriangles")

    println(s"9. reduce increment messages for each node")
    // (nodeId, #triangles)
    val tcRdd = resRdd.flatMap(row => row._3).reduceByKey(_ + _).persist($(storageLevel))

    println(s"======sampled degreeRdd======")
    degreeRdd.take(20).foreach(f =>
      println(s"${f._1} : ${f._2}")
    )

    println(s"======sampled tcRdd======")
    tcRdd.take(20).foreach{ case(node, tc) =>
      println(s"$node : $tc")
    }

    println(s"10. calculate LCC for each node")
    val outputRdd = degreeRdd.join(tcRdd).map { case (ndoeId, (n, tc)) =>
        val lcc: Double = if (n > 1) tc.toDouble / (n * (n - 1)) else 0.0
        Row(ndoeId, lcc)
    }

    println(s"======sampled output======")
    outputRdd.take(20).foreach { row => println(row.getLong(0) +":" + " lcc: " + row.getDouble(1)) }

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(outputRdd, outputSchema)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField($(srcNodeIdCol), LongType, nullable = false),
      StructField($(srcNodeLccCol), DoubleType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}



