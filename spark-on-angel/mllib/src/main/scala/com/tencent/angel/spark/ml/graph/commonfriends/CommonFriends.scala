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

package com.tencent.angel.spark.ml.graph.commonfriends

import com.tencent.angel.ml.math2.vector.{IntLongVector, LongIntVector}
import com.tencent.angel.spark.ml.graph.params._
import com.tencent.angel.spark.ml.graph.utils.NodeIndexer
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.storage.StorageLevel


class CommonFriends(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize
  with HasBufferSize with HasCommonFriendsNumCol with HasDebugMode {

  def this() = this(Identifiable.randomUID("commonfriends"))

  override def transform(dataset: Dataset[_]): DataFrame = {

    assert(dataset.sparkSession.sparkContext.getCheckpointDir.nonEmpty, "set checkpoint dir first")
    val rawEdges: RDD[(Long, Long)] = {
      dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd.map { row =>
        (row.getLong(0), row.getLong(1))
      }
    }.repartition($(partitionNum)).persist(StorageLevel.DISK_ONLY)

    val nodes = rawEdges.flatMap { case (src, dst) =>
      Iterator(src, dst)
    }.distinct($(partitionNum))

    val reIndexer = new NodeIndexer()
    reIndexer.train($(psPartitionNum), nodes)

    val edges: RDD[(Int, Int)] = reIndexer.encode(rawEdges, 100) { case (iter, ps) =>
      val keys = iter.flatMap { case (src, dst) => Iterator(src, dst) }.distinct
      val map = ps.pull(keys).asInstanceOf[LongIntVector]
      iter.map { case (src, dst) =>
        (map.get(src), map.get(dst))
      }.toIterator
    }

    //val numEdges = edges.mapPartitions( iter => Iterator.single(iter.length)).sum()
    val numNodes = reIndexer.getNumNodes
    //println(s"numEdge=$numEdges")
    println(s"maxNodeId=$numNodes")

    val partitions: RDD[CommonFriendsPartition] =
      CommonFriendsGraph.edgeRDD2GraphPartitions(edges, numNodes, Some($(partitionNum)),storageLevel = $(storageLevel))

    // destroys the lineage and close encoder of node indexer
    partitions.checkpoint()
    partitions.foreachPartition(_ => Unit)
    //reIndexer.destroyEncoder()

    rawEdges.unpersist()

    val psModel = CommonFriendsPSModel(numNodes, $(batchSize), $(pullBatchSize), $(psPartitionNum))
    psModel.initNeighborTable(edges)
    val graph = new CommonFriendsGraph(partitions, psModel)

    val nodeIds= Array(1,2,3)
    val nodeNeighbors = psModel.getNeighborTable(nodeIds)
    val iter = nodeNeighbors.int2ObjectEntrySet().fastIterator()
    while(iter.hasNext) {
      val entry = iter.next()
      println(s"node id = ${entry.getIntKey}, neighbors = ${entry.getValue.mkString(",")}")
    }

    println(s"total degree: ${graph.getDegree().sum()}")

    val rawResult: RDD[((Int, Int), Int)] = graph.run()

    val decodeResult: RDD[Row] = reIndexer.decode(rawResult, 100) { case (iter, ps) =>
      val keys = iter.flatMap { case ((src, dst), numFriends) => Iterator(src, dst) }.distinct
      val map = ps.pull(keys).asInstanceOf[IntLongVector]
      iter.map { case ((src, dst), numFriends) =>
        Row(map.get(src), map.get(dst), numFriends.toInt)
      }.toIterator
    }

    println(s"======results with original node index======")
    decodeResult.take(10).foreach( triple => println(triple.mkString(",")))
    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(decodeResult, outputSchema)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField($(srcNodeIdCol), LongType, nullable = false),
      StructField($(dstNodeIdCol), LongType, nullable = false),
      StructField($(numCommonFriendsCol), IntegerType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

object CommonFriends {


}
