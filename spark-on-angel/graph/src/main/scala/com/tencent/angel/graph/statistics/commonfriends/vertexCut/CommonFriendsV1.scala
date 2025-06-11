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
package com.tencent.angel.graph.statistics.commonfriends.vertexCut

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.data.neighbor.NeighborDataOps
import com.tencent.angel.graph.model.neighbor.dynamic.DynamicNeighborModel
import com.tencent.angel.graph.utils.{BatchIter, GraphIO, PartitionTools}
import com.tencent.angel.graph.utils.Stats.{summarizeApplyOp, summarizeReduceOp}
import com.tencent.angel.graph.utils.params._
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.utils.ArrayUtils
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.types.{IntegerType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.mutable

class CommonFriendsV1(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasCompressCol
  with HasIsCompressed with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize
  with HasBufferSize with HasCommonFriendsNumCol with HasDebugMode
  with HasSrcNodeIndex with HasDstNodeIndex with HasCompressIndex
  with HasInput with HasExtraInputs with HasDelimiter {

  def this() = this(Identifiable.randomUID("commonFriends"))

  private var maxComFriendsNum: Int = Int.MaxValue
  def setMaxComFriendsNum(in: Int): Unit = { this.maxComFriendsNum = in }

  private var approxNumNodes: Long = -1L
  def setNumNodes(in: Long): Unit = { this.approxNumNodes = in }

  override def transform(dataset: Dataset[_]): DataFrame = {

    val sc = dataset.sparkSession.sparkContext

    println(s"======load edges from the first input======")
    val beforeReadTime = System.currentTimeMillis()
    val firstEdges_ : RDD[(Long, Long)] = {
      if ($(isCompressed)) {
        NeighborDataOps.loadCompressedEdges(dataset, $(srcNodeIdCol), $(dstNodeIdCol), $(compressCol))
      } else {
        NeighborDataOps.loadEdges(dataset, $(srcNodeIdCol), $(dstNodeIdCol))
      }
    }
    val inputPartNum = firstEdges_.getNumPartitions
    val firstEdges =
      if (inputPartNum >= $(partitionNum))
        firstEdges_.coalesce($(partitionNum))
      else
        firstEdges_.repartition($(partitionNum))
    firstEdges.persist($(storageLevel))

    println(s"======sample edges======")
    println(firstEdges.take(10).mkString(","))

    val beforeStats = System.currentTimeMillis()
    val (minId, maxId, numEdges) = firstEdges.mapPartitions(summarizeApplyOp).reduce(summarizeReduceOp)
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges")
    println(s"stats cost ${System.currentTimeMillis() - beforeStats} ms.")

    val nodeNum = approxNumNodes
    println(s"approx distinct node num=$nodeNum")
    println(s"======start parameter server======")
    val psStartTime = System.currentTimeMillis()
    startPS(dataset.sparkSession.sparkContext)
    println(s"start parameter server costs ${System.currentTimeMillis() - psStartTime} ms")

    println(s"======push neighbor tables to parameter server======")
    val initTableStartTime = System.currentTimeMillis()
    val modelContext = new ModelContext($(psPartitionNum), minId, maxId + 1L, nodeNum,
      "dynamic_neighbor", sc.hadoopConfiguration)
    val model = new DynamicNeighborModel(modelContext)
    model.init()

    val initNumEdges = model.initNeighbors(firstEdges, $(batchSize))
    println(s"init $initNumEdges edges.")

    println(s"start pulling nodes.")
    val beforePullNodesTime = System.currentTimeMillis()
    val nodes = model.getNodes($(psPartitionNum)).repartition($(partitionNum)).persist(StorageLevel.MEMORY_ONLY)
    println(s"pulled ${nodes.count()} nodes, cost ${System.currentTimeMillis() - beforePullNodesTime} ms.")

    val beforeTransTime = System.currentTimeMillis()
    println(s"start get and sort processing.")
    val transNum = model.trans(nodes, $(pullBatchSize) * 10)
    println(s"processed $transNum nodes with dynamic neighbors, cost ${System.currentTimeMillis() - beforeTransTime} ms.")

    println(s"initializing the neighbor table cost ${System.currentTimeMillis() - initTableStartTime} ms")
    val cpTableStartTime = System.currentTimeMillis()
    model.checkpoint()
    println(s"checkpoint of neighbor table cost ${System.currentTimeMillis() - cpTableStartTime} ms")

    println(s"======load edges from the second input======")
    val extraInput = $(extraInputs)
    assert(extraInput.length == 1, s"multiple inputs for non-friends are not supported")
    val isOneInput = extraInput(0).equals($(input))

    val secondDF: DataFrame = if (!isOneInput) {
      GraphIO.load(extraInput(0),
        isWeighted = false,
        srcIndex = $(srcNodeIndex),
        dstIndex = $(dstNodeIndex),
        sep = $(delimiter))
    } else null

    val partitioner = PartitionTools.edge2DPartitioner($(partitionNum))
    val secondEdges: RDD[(Long, Long)] = if (!isOneInput) {
      secondDF.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd.mapPartitions { iter =>
        iter.flatMap { row =>
          if (row.getLong(0) == row.getLong(1))
            Iterator.empty
          else
            Iterator.single((row.getLong(0), row.getLong(1)), 0)
        }
      }.repartitionAndSortWithinPartitions(partitioner)
        .map(_._1)
        .persist($(storageLevel))
    } else sc.emptyRDD

    val numSecondEdges = secondEdges.count()
    println(s"num of edges in the second input = $numSecondEdges")

    println(s"======sample edges in the second input======")
    println(secondEdges.take(10).mkString(","))

    println(s"======start calculation======")
    val rawResult: RDD[Row] = if (!isOneInput) {
      secondEdges.mapPartitionsWithIndex { case (partId, iter) =>
        CommonFriendsV1.runEdgePartition(iter, partId, $(pullBatchSize), model, maxComFriendsNum)
      }
    } else {
      nodes.mapPartitionsWithIndex { case (partId, iter) =>
        CommonFriendsV1.runNeighborPartition(iter, partId, $(pullBatchSize), model, maxComFriendsNum)
      }
    }

    println(s"======sample results======")
    rawResult.take(10).foreach { row =>
      println(s"src = ${row.getLong(0)}, dst = ${row.getLong(1)}, num of common friends = ${row.getInt(2)}")
    }

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(rawResult, outputSchema)
  }

  def startPS(sc: SparkContext): Unit = {
    PSContext.getOrCreate(sc)
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

object CommonFriendsV1 {

  def runEdgePartition(iter: Iterator[(Long, Long)], partitionId: Int, batchSize: Int, model: DynamicNeighborModel,
                       maxComFriendsNum: Int = Int.MaxValue): Iterator[Row] = {
    var totalRowNum = 0
    var totalPullNum = 0
    var startTs = System.currentTimeMillis()
    BatchIter(iter, batchSize).flatMap { batchIter =>
      println(s"partition $partitionId: last batch cost ${System.currentTimeMillis() - startTs} ms")
      startTs = System.currentTimeMillis()
      val pullNodes: mutable.HashSet[Long] = new mutable.HashSet[Long]()
      batchIter.foreach { curEdge =>
        pullNodes.add(curEdge._1)
        pullNodes.add(curEdge._2)
      }
      val beforePullTs = System.currentTimeMillis()
      val neighborsNodesMap = model.getNeighbors(pullNodes.toArray)
      totalRowNum += batchSize
      totalPullNum += pullNodes.size
      println(s"partition $partitionId process ${batchIter.length} edges ($totalRowNum in total), " +
        s"pull ${pullNodes.size} nodes from ps ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")
      batchIter.toIterator.flatMap { case (src, dst) =>
        val srcNeighbors = neighborsNodesMap.get(src)
        val dstNeighbors = neighborsNodesMap.get(dst)
        Iterator.single(Row(src, dst, ArrayUtils.intersectCountWithLimits(srcNeighbors, dstNeighbors, maxComFriendsNum)))
      }
    }
  }

  def runNeighborPartition(iter: Iterator[Long], partitionId: Int, batchSize: Int, model: DynamicNeighborModel,
                           maxComFriendsNum: Int = Int.MaxValue): Iterator[Row] = {
    var totalRowNum = 0L
    var totalPullNum = 0L
    var startTs = System.currentTimeMillis()
    BatchIter(iter, batchSize).flatMap { batchIter =>
      println(s"partition $partitionId: last batch cost ${System.currentTimeMillis() - startTs} ms")
      startTs = System.currentTimeMillis()
      val pullNodes: mutable.HashSet[Long] = new mutable.HashSet[Long]()
      val localNeighborTable: Long2ObjectOpenHashMap[Array[Long]] = model.getNeighbors(batchIter)
      batchIter.foreach { src => pullNodes ++= localNeighborTable.get(src) }
      val beforePullTs = System.currentTimeMillis()
      val psNeighborsTable = model.getNeighbors(pullNodes.toArray)
      totalRowNum += batchIter.length
      totalPullNum += pullNodes.size
      println(s"partition $partitionId: process ${batchIter.length} neighbor tables ($totalRowNum in total), " +
        s"pull ${pullNodes.size} nodes from ps ($totalPullNum in total), " +
        s"cost ${System.currentTimeMillis() - beforePullTs} ms")
      batchIter.flatMap { src =>
        val srcNeighbors = localNeighborTable.get(src)
        srcNeighbors.flatMap { dst =>
          val dstNeighbors = psNeighborsTable.get(dst)
          Iterator.single(Row(src, dst, ArrayUtils.intersectCountWithLimits(srcNeighbors, dstNeighbors, maxComFriendsNum)))
        }
      }
    }
  }
}
