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

import com.tencent.angel.spark.ml.graph.params._
import com.tencent.angel.spark.ml.graph.utils.{BatchIter, PartitionTools}
import com.tencent.angel.utils.ArrayUtils
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class CommonFriendsV4(override val uid: String) extends Transformer
  with HasWeightCol with HasSrcNodeIdCol with HasDstNodeIdCol
  with HasIsWeighted with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize
  with HasBufferSize with HasCommonFriendsNumCol with HasDebugMode {

  def this() = this(Identifiable.randomUID("commonfriends"))

  override def transform(dataset: Dataset[_]): DataFrame = {

    assert(dataset.sparkSession.sparkContext.getCheckpointDir.nonEmpty, "set checkpoint dir first")

    val numPart = dataset.sparkSession.sparkContext.getConf.getInt("spark.default.parallelism", $(partitionNum))

    println(s"default parallelism: $numPart")
    println(s"partition number: ${$(partitionNum)}")

    val partitioner = PartitionTools.edge2DPartitioner($(partitionNum))
    val edges: RDD[(Long, Long)] =
      dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd.flatMap { row =>
        if (row.getLong(0) == row.getLong(1)) {
          Iterator.empty
        } else {
          Iterator.single((row.getLong(0), row.getLong(1)), 0)
        }
      }.repartitionAndSortWithinPartitions(partitioner)
      .map(_._1)
      .persist($(storageLevel))

    println(s"edges partition number = ${edges.getNumPartitions}")
    val numEdges = edges.count()
    println(s"num of edges = $numEdges")

    val neighborsRDD: RDD[(Long, Array[Long])] =
      edges.groupByKey($(partitionNum)).mapPartitions { iter =>
        if (iter.nonEmpty) {
          iter.flatMap { case (key, group) =>
            Iterator.single(key, group.toArray.sorted)
          }
        } else {
          Iterator.empty
        }
      }

    neighborsRDD.foreachPartition(_ => Unit)

    println(s"neighborsRDD partition number = ${neighborsRDD.getNumPartitions}")
    val numNodes = neighborsRDD.count()
    println(s"num of nodes = $numNodes")

    val (minNodeId, maxNodeId) = edges.mapPartitions { iter =>
      var min = Long.MaxValue
      var max = Long.MinValue
      iter.foreach { case(src, dst) =>
        min = math.min(min, math.min(src, dst))
        max = math.max(max, math.max(src, dst))
      }
      Iterator.single((min, max))
    }.reduce{ case (c1: (Long, Long), c2: (Long, Long)) =>
      (c1._1 min c2._1, c1._2 max c2._2)
    }

    println(s"max node id = $maxNodeId")
    println(s"min node id = $minNodeId")

    val initStartTime = System.currentTimeMillis()
    val psModel = CommonFriendsPSModel(maxNodeId + 1, $(batchSize), $(pullBatchSize), $(psPartitionNum))
    psModel.initLongNeighborTable(neighborsRDD)
    println(s"initializing the neighbor table costs ${System.currentTimeMillis() - initStartTime} ms")
    val cpStartTime = System.currentTimeMillis()
    psModel.checkpoint()
    println(s"checkpoint of neighbor table costs ${System.currentTimeMillis() - cpStartTime} ms")

    val nodeIds = neighborsRDD.take(3).map(_._1)
    val nodeNeighbors = psModel.getLongNeighborTable(nodeIds)
    val iter = nodeNeighbors.long2ObjectEntrySet().fastIterator()
    while(iter.hasNext) {
      val entry = iter.next()
      println(s"node id = ${entry.getLongKey}, neighbors = ${entry.getValue.mkString(",")}")
    }

    val rawResult: RDD[Row] = edges.mapPartitionsWithIndex { case (partId, iter) =>
      CommonFriendsV4.runEdgePartition(iter, partId, psModel)
    }

    println(s"======results======")
    rawResult.take(10).foreach{ row =>
      println(s"src = ${row.getLong(0)}, ds = ${row.getLong(1)}, num of common friends = ${row.getInt(2)}")
    }

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(rawResult, outputSchema)
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

object CommonFriendsV4 {

  def runEdgePartition(iter: Iterator[(Long, Long)], partitionId: Int, psModel: CommonFriendsPSModel): Iterator[Row] = {
    val batchSize = psModel.neighborTable.param.pullBatchSize
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    BatchIter(iter, batchSize).flatMap { batchIter =>
      println(s"last batch cost ${System.currentTimeMillis() - startTs} ms")
      startTs = System.currentTimeMillis()
      val edgeBuffer: ArrayBuffer[(Long, Long)] = new ArrayBuffer[(Long, Long)]()
      val pullNodes: mutable.HashSet[Long] = new mutable.HashSet[Long]()
      batchIter.foreach { curEdge =>
        pullNodes.add(curEdge._1)
        pullNodes.add(curEdge._2)
        edgeBuffer += curEdge
      }
      val beforePullTs = System.currentTimeMillis()
      val neighborsNodesMap = psModel.getLongNeighborTable(pullNodes.toArray)
      totalRowNum += batchSize
      totalPullNum += pullNodes.size
      println(s"partition $partitionId process $batchSize edges ($totalRowNum in total), " +
        s"pull ${pullNodes.size} nodes from ps ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")
      edgeBuffer.toIterator.flatMap { case (src, dst) =>
        val srcNeighbors = neighborsNodesMap.get(src)
        val dstNeighbors = neighborsNodesMap.get(dst)
        Iterator.single(Row(src, dst, ArrayUtils.intersectCount(srcNeighbors, dstNeighbors)))
      }
    }
  }
}
