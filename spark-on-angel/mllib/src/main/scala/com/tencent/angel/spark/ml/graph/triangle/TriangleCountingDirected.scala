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

import com.tencent.angel.spark.ml.graph.data.CounterTriangleDirected
import com.tencent.angel.spark.ml.graph.params._
import com.tencent.angel.spark.ml.graph.{GraphOps, NeighborTableModel}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


class TriangleCountingDirected(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize
  with HasBufferSize with HasCommonFriendsNumCol with HasDebugMode
  with HasSrcNodeIndex with HasDstNodeIndex
  with HasInput with HasExtraInputs with HasDelimiter with HasOutputTriangleCol {

  def this() = this(Identifiable.randomUID("triangle_counting_directed"))

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
    val neighborRDD = GraphOps.edgesToNeighborTableWithByteTags(edges, $(partitionNum))
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

    neighborModel.initLongNeighborByteAttr(neighborPartitions)
    println(s"push costs ${System.currentTimeMillis() - initTableStartTime} ms")
//    val cpTableStartTime = System.currentTimeMillis()
//    println(s"7. checkpoint of neighbor table")
//    neighborModel.checkpoint()
//    println(s"checkpoint costs ${System.currentTimeMillis() - cpTableStartTime} ms")

    println(s"8. calculate triangle counting")
    val resRdd = neighborModel.calTriangleDirected(neighborPartitions).persist($(storageLevel))

    val totalTriangles: Long = resRdd.map(row => row._2).reduce(_ + _)
    println(s"==> total num of triangles directed = $totalTriangles")

    println(s"9. reduce increment messages for each node")
    // (nodeId, #triangles)
    val tcRdd = resRdd.flatMap(row => row._3).reduceByKey { case (cnt1, cnt2) =>
        val sum = new CounterTriangleDirected(7)
        for (i <- cnt1.indices) {
          sum(i) = cnt1(i) + cnt2(i)
        }
        sum
      }

    println(s"10. transform RDD[(nodeId, #tc)] to RDD[Row]")
    val output = tcRdd.map { case (nodeId, tc) =>
        Row(nodeId, tc(0), tc(1), tc(2), tc(3), tc(4), tc(5), tc(6))
    }

    println(s"======sampled output======")
    output.take(20).foreach { row => println(row.getLong(0)
      +": [" + row.getInt(1) + "," + row.getInt(2) + "," + row.getInt(3)
      + "," + row.getInt(4) + "," + row.getInt(5) + "," + row.getInt(6) + "," + row.getInt(7) + "]") }

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(output, outputSchema)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField($(srcNodeIdCol), LongType, nullable = false),
      StructField("tc0", IntegerType, nullable = false),
      StructField("tc1", IntegerType, nullable = false),
      StructField("tc2", IntegerType, nullable = false),
      StructField("tc3", IntegerType, nullable = false),
      StructField("tc4", IntegerType, nullable = false),
      StructField("tc5", IntegerType, nullable = false),
      StructField("tc6", IntegerType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}


object TriangleCountingDirected {

  /**
    * @param srcEdges sorted by nodeId
    * @param dstEdges sorted by nodeId
    * @tparam ED  type of attribute on edge
    * @return (nodeId, (src_tag, dst_tag))
    */
  def intersect[ED: ClassTag](srcEdges: Array[(Long, ED)], dstEdges: Array[(Long, ED)]): Array[(Long, (ED, ED))] = {

    val res = new ArrayBuffer[(Long, (ED, ED))]


    if (srcEdges == null || dstEdges == null || srcEdges.length == 0 || dstEdges.length == 0)
      return Array()

    var i = 0
    var j = 0

    while (i < srcEdges.length && j < dstEdges.length) {
      if (srcEdges(i)._1 < dstEdges(j)._1) i += 1
      else if (srcEdges(i)._1 > dstEdges(j)._1) j += 1
      else {
        res += ((srcEdges(i)._1, (srcEdges(i)._2, dstEdges(j)._2)))
        i += 1
        j += 1
      }
    }

    res.toArray
  }
}
