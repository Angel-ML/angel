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
package com.tencent.angel.spark.ml.graph.algo.statistics.commonfriends

import com.tencent.angel.graph.data.neighbor.NeighborDataOps
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.utils.params._
import com.tencent.angel.spark.ml.graph.utils.BatchIter
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types._

import scala.collection.mutable

class NonLinkComFriends(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasCompressCol
  with HasIsCompressed with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize
  with HasBufferSize with HasCommonFriendsNumCol with HasDebugMode
  with HasSrcNodeIndex with HasDstNodeIndex with HasCompressIndex
  with HasDelimiter with HasUseBalancePartition with HasNeedReplicaEdge with HasTopK {

  def this() = this(Identifiable.randomUID("commonFriends"))

  override def transform(dataset: Dataset[_]): DataFrame = {

    val sc = dataset.sparkSession.sparkContext

    assert(sc.getCheckpointDir.nonEmpty, "set checkpoint dir first")

    val numPart = sc.getConf.getInt("spark.default.parallelism", $(partitionNum))
    println(s"default parallelism: $numPart")
    println(s"partition number: ${$(partitionNum)}")

    println(s"======load edges from the first input======")
    val firstEdges: RDD[(Long, Long)] = {
      if ($(isCompressed)) {
        NeighborDataOps.loadCompressedEdges(dataset, $(srcNodeIdCol), $(dstNodeIdCol), $(compressCol))
      } else {
        NeighborDataOps.loadEdges(dataset, $(srcNodeIdCol), $(dstNodeIdCol), $(needReplicaEdge))
      }
    }.persist($(storageLevel))

    println(s"======sample edges======")
    println(firstEdges.take(10).mkString(","))

    val firstNeighbors = NeighborDataOps.edges2NeighborTable(firstEdges, $(partitionNum))
      .persist($(storageLevel))
    println(s"======statistics of the data (followers)======")
    val stats = NeighborDataOps.statsByNeighborTable(firstNeighbors)
    println(s"min node id = ${stats._1}")
    println(s"max node id = ${stats._2}")
    println(s"num of nodes = ${stats._3}")
    println(s"num of edges = ${stats._4}")
    println(s"min degree = ${stats._5}")
    println(s"max degree = ${stats._6}")
    println(s"======start parameter server======")
    val psStartTime = System.currentTimeMillis()
    startPS(dataset.sparkSession.sparkContext)
    println(s"start parameter server costs ${System.currentTimeMillis() - psStartTime} ms")

    println(s"======push neighbor tables to parameter server======")
    val initTableStartTime = System.currentTimeMillis()
    val psModel = CommonFriendsPSModel(stats._2 + 1, $(batchSize), $(pullBatchSize), $(psPartitionNum),
      stats._1, useBalancePartition = $(useBalancePartition))
    psModel.initLongNeighborTable(firstNeighbors)
    println(s"initializing the neighbor table costs ${System.currentTimeMillis() - initTableStartTime} ms")
    val cpTableStartTime = System.currentTimeMillis()
    psModel.checkpoint()
    println(s"checkpoint of neighbor table costs ${System.currentTimeMillis() - cpTableStartTime} ms")


    //    CommonFriendsOperator.testPS(firstNeighbors, psModel, 10)
    //    val checkValid = CommonFriendsOperator.checkValid(firstNeighbors, psModel, 10)
    //    require(checkValid, s"result with executor RDD and that with PS are different")

    println(s"======start calculation======")
    val rawResult = firstNeighbors.mapPartitionsWithIndex { case (partId, iter) =>
      NonLinkComFriends.runNeighborPartition(partId, iter, psModel, $(topK))
    }
    val re = rawResult.map(x => Row(x._1, x._2))

    //    println(s"======sample results======")
    //    re.take(10).foreach { row =>
    //      println(s"src = ${row.getLong(0)}, dst = ${row.getLong(1)}, num of common friends = ${row.getInt(2)}")
    //    }

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(re, outputSchema)
  }

  def startPS(sc: SparkContext): Unit = {
    PSContext.getOrCreate(sc)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField($(srcNodeIdCol), LongType, nullable = false),
      StructField("comFriends", StringType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

object NonLinkComFriends {

  def runNeighborPartition(partId: Int, iter: Iterator[(Long, Array[Long])],
                           psModel: CommonFriendsPSModel, top: Int = 3): Iterator[(Long, String)] = {
    val batchSize = psModel.neighborTable.param.pullBatchSize
    var startTs = System.currentTimeMillis()
    var computeStartTs = System.currentTimeMillis()

    BatchIter(iter, batchSize).flatMap { batchIter =>
      println(s"partition $partId: last batch cost ${System.currentTimeMillis() - startTs} ms, " +
        s"last computation cost ${System.currentTimeMillis() - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      var numUsers = 0
      val pullNodes = new mutable.HashSet[Long]()
      val localNeighborTable = new Long2ObjectOpenHashMap[Array[Long]](batchSize) // user-items
      batchIter.foreach { case (user, neighbors) =>
        numUsers += 1
        localNeighborTable.put(user, neighbors)
        pullNodes ++= neighbors
      }
      val beforePullTs = System.currentTimeMillis()
      val psNeighborTable = psModel.getLongNeighborTable(pullNodes.toArray)
      println(s"partition $partId: process $numUsers neighbor tables, " +
        s"pull ${pullNodes.size} nodes from ps, " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")

      val srcNodes = localNeighborTable.keySet().toLongArray()
      computeStartTs = System.currentTimeMillis()

      srcNodes.flatMap { src =>
        val srcNbrs = localNeighborTable.get(src)
        val srcFollowingSet = localNeighborTable.get(src).toSet
        val occurrence = new mutable.OpenHashMap[Long, Int]()
        srcNbrs.foreach { dst =>
          psNeighborTable.get(dst).foreach { i =>
            if (i > src && !srcFollowingSet.contains(i)) {
              val times = occurrence.getOrElse(i, 0)
              occurrence.update(i, times + 1)
            }
          }
        }
        val occ = occurrence.filter(_._2 > top)
        if (occ.nonEmpty) {
          val recall = occ.toArray.sortWith(_._2 > _._2).map(x => x._1 + ":" + x._2).mkString(" ")
          Iterator.single(src, recall)
//          occ.flatMap { case (n, num) => Iterator((src, n, num), (n, src, num)) }
        }
        else Iterator.empty
      }
    }
  }
}
