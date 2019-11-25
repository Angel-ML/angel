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

import com.tencent.angel.spark.ml.graph.params._
import com.tencent.angel.spark.ml.graph.{GraphOps, NeighborTableModel}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class TriangleCountingUndirected(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize
  with HasBufferSize with HasCommonFriendsNumCol with HasDebugMode
  with HasSrcNodeIndex with HasDstNodeIndex
  with HasInput with HasExtraInputs with HasDelimiter with HasOutputTriangleCol
  with HasSrcNodeLccCol {

  def this() = this(Identifiable.randomUID("triangle_counting_undirected"))

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
//    println(s"sample neighbor table = ")
//    neighborRDD.take(10).foreach(println)

    println(s"3. build neighbor table partition")
    val neighborPartitions = GraphOps.buildNeighborTablePartition(neighborRDD)
    neighborPartitions.persist($(storageLevel))

    println(s"4: calculate stats of graph")
    val stats = GraphOps.getStats(neighborPartitions)
    println(stats)

    println(s"5：start parameter server")
    val psStartTime = System.currentTimeMillis()
    NeighborTableModel.startPS(dataset.sparkSession.sparkContext)
    println(s"start parameter server costs ${System.currentTimeMillis() - psStartTime} ms")

    println(s"6：push neighbor tables to parameter server")
    val initTableStartTime = System.currentTimeMillis()
    val neighborModel = NeighborTableModel(stats.maxVertexId + 1, $(batchSize), $(pullBatchSize), $(psPartitionNum))

    neighborModel.initLongNeighbor(neighborPartitions)
    neighborModel.testPS(neighborPartitions)
    println(s"push costs ${System.currentTimeMillis() - initTableStartTime} ms")
//    val cpTableStartTime = System.currentTimeMillis()
//    println(s"7. checkpoint of neighbor table")
//    neighborModel.checkpoint()
//    println(s"checkpoint costs ${System.currentTimeMillis() - cpTableStartTime} ms")

    println(s"8. calculate triangle counting")
    val resRdd = neighborModel.calTriangleUndirected(neighborPartitions).persist($(storageLevel))
    val totalTriangles: Long = resRdd.map(row => row._2).reduce(_ + _)
    println(s"==> total num of triangles = $totalTriangles")

    // (nodeId, #triangles)
    println(s"9. reduce increment messages for each node")
    val tcRdd = resRdd.flatMap(row => row._4).reduceByKey(_ + _).persist($(storageLevel))


    // calculate LCC
    println(s"10. calculate LCC for each node")
    val outputRdd = resRdd.map( r => (r._1, r._3) ).join(tcRdd).map { case (nodeId, (n, tc)) =>
      val lcc: Double = if (n > 1) (2.0 * tc) / (n * (n - 1)) else 0.0
      // (src, #tc, lcc)
      Row(nodeId, lcc, tc)
    }

    resRdd.unpersist()

    println(s"======sampled output======")
    outputRdd.take(10).foreach { row => println(row.getLong(0) + ": lcc: " + row.getDouble(1)
      + ", #tc: " + row.getLong(2) ) }

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(outputRdd, outputSchema)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField($(srcNodeIdCol), LongType, nullable = false),
      StructField($(srcNodeLccCol), DoubleType, nullable = false),
      StructField($(outputTriangleCol), LongType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}
