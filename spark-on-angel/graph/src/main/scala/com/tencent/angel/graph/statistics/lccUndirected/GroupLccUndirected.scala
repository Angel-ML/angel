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
package com.tencent.angel.graph.statistics.lccUndirected

import com.tencent.angel.graph.common.param.ModelContext
import com.tencent.angel.graph.model.neighbor.simple.SimpleNeighborTableModel
import com.tencent.angel.graph.utils.Stats
import com.tencent.angel.graph.utils.collection.OpenHashSet
import com.tencent.angel.spark.context.PSContext
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel
import com.tencent.angel.graph.utils.params._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


class GroupLccUndirected(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasCompressCol
  with HasIsCompressed with HasPartitionNum with HasPSPartitionNum
  with HasStorageLevel with HasBatchSize with HasPullBatchSize
  with HasBufferSize with HasCommonFriendsNumCol with HasDebugMode
  with HasSrcNodeIndex with HasDstNodeIndex with HasCompressIndex
  with HasInput with HasExtraInputs with HasDelimiter {

  def this() = this(Identifiable.randomUID("lccUndirected"))

  var userGroup: RDD[(Long, Long)] = _
  def setUserGroup(in: RDD[(Long, Long)]): Unit = { this.userGroup = in }

  override def transform(dataset: Dataset[_]): DataFrame = {

    println(s"======load edges from the first input======")
    val firstEdges: RDD[(Long, Long)] = LccOperator.loadEdges(dataset, $(srcNodeIdCol), $(dstNodeIdCol))
    firstEdges.persist(StorageLevel.DISK_ONLY)

    println(s"======statistics of the data======")
    val (minId, maxId, numEdges) = firstEdges.mapPartitions(Stats.summarizeApplyOp).reduce(Stats.summarizeReduceOp)
    println(s"minId: $minId, maxId: $maxId, numEdges: $numEdges")

    println(s"======start parameter server======")
    val psStartTime = System.currentTimeMillis()
    startPS(dataset.sparkSession.sparkContext)
    println(s"start parameter server costs ${System.currentTimeMillis() - psStartTime} ms")

    // create user-group ps matrix
    val tagContext = new ModelContext($(psPartitionNum),  minId, maxId+1, -1, "tag", SparkContext.getOrCreate().hadoopConfiguration)
    val model = new SimpleNeighborTableModel(tagContext)
    model.init()

    val beforeInitTag = System.currentTimeMillis()
    userGroup.groupByKey($(partitionNum)).map(x => (x._1, x._2.toArray.distinct.sorted))
        .mapPartitions { iter => iter.sliding($(batchSize), $(batchSize))
          .map(pairs => model.initNeighbors(pairs))
        }.count()
    println(s"push tag cost ${System.currentTimeMillis() - beforeInitTag} ms")

    val cpTableStartTime = System.currentTimeMillis()
    model.checkpoint()
    println(s"checkpoint of neighbor table costs ${System.currentTimeMillis() - cpTableStartTime} ms")

    // filter out valid edges whose src and dst are in the same groups
    val originPartNum = firstEdges.getNumPartitions
    val edges = { if (originPartNum >= $(partitionNum))
      firstEdges.coalesce($(partitionNum))
    else
      firstEdges.repartition($(partitionNum))
    }.mapPartitionsWithIndex { case (index, iter) =>
      GroupLccUndirected.filterEdges(index, iter, $(pullBatchSize), model)
    }

    // calc lcc by groups
    val result = edges.groupByKey($(partitionNum)).mapPartitionsWithIndex { case (index, iter) =>
      var startTs = System.currentTimeMillis()
      iter.sliding(10000, 10000).flatMap { batchIter =>
        val endTs = System.currentTimeMillis()
        println(s"calc group lcc: partition $index: last batch total_time: ${endTs - startTs} ms.")
        startTs = System.currentTimeMillis()
        batchIter.map { case (groupId, data) =>
          val (lcc, numTriangleEdges, numNodes) = GroupLccUndirected.calcTriangle(data)
          Row(groupId, lcc, numTriangleEdges, numNodes)
        }
      }
    }

    println(s"======sample results======")
    result.take(10).foreach { row =>
      println(s"groupId=${row.getLong(0)}, groupLcc=${row.getFloat(1)}, numTriangleEdges=${row.getInt(2)}, numNodes=${row.getInt(3)}")
    }

    val outputSchema = transformSchema(dataset.schema)
    dataset.sparkSession.createDataFrame(result, outputSchema)
  }

  def startPS(sc: SparkContext): Unit = {
    PSContext.getOrCreate(sc)
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(
      StructField("groupId", LongType, nullable = false),
      StructField("lcc", FloatType, nullable = false),
      StructField("numTriangleEdges", IntegerType, nullable = false),
      StructField("numNodes", IntegerType, nullable = false)
    ))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}

object GroupLccUndirected {
  def intersect(src: Array[Long], dst: Array[Long]): Array[Long] = {
    if (src == null || src.length == 0 || dst == null || dst.length == 0)
      Array[Long]()
    else {
      val count = new ArrayBuffer[Long]()
      var i = 0
      var j = 0
      while (i < src.length && j < dst.length) {
        if (src(i) < dst(j)) i += 1
        else if (src(i) > dst(j)) j += 1
        else {
          count.append(src(i))
          i += 1
          j += 1
        }
      }
      count.toArray
    }
  }

  def filterEdges(index: Int, iter: Iterator[(Long, Long)], batchSize: Int, model: SimpleNeighborTableModel): Iterator[(Long, (Long, Long))] = {
    var startTs = System.currentTimeMillis()
    var computeStartTs = System.currentTimeMillis()
    iter.sliding(batchSize, batchSize).flatMap { batchIter =>
      val endTs = System.currentTimeMillis()
      println(s"filter edges: partition $index: last batch total_time: ${endTs - startTs} ms, compute_time: ${endTs - computeStartTs} ms")
      startTs = System.currentTimeMillis()
      val pullNodes = new mutable.HashSet[Long]()
      batchIter.foreach { case (src, dst) =>
        pullNodes.add(src)
        pullNodes.add(dst)
      }
      val beforePullTs = System.currentTimeMillis()
      val pulledTags = model.getNeighbors(pullNodes.toArray)
      println(s"pulled ${pullNodes.size} tag, cost ${System.currentTimeMillis() - beforePullTs} ms.")
      computeStartTs = System.currentTimeMillis()
      batchIter.flatMap { case (src, dst) =>
        val commons = intersect(pulledTags.get(src), pulledTags.get(dst))
        if (commons.nonEmpty) {
          commons.map(x => (x, (src, dst))).toIterator
        } else Iterator.empty
      }
    }
  }

  def calcTriangle(edges: Iterable[(Long, Long)]): (Float, Int, Int) = {
    var before = System.currentTimeMillis()
    val (neighborTable, numNodes) = getNeighborTable(edges)
    before = System.currentTimeMillis()
    val triangleEdges = new OpenHashSet[(Long, Long)]()
    neighborTable.keySet().toLongArray().foreach { src =>
      val srcNbrWgts = neighborTable.get(src)
      srcNbrWgts.foreach { dst =>
        val dstNbrWgts = neighborTable.get(dst)
        val commons = intersect(srcNbrWgts, dstNbrWgts)
        if (commons.length > 0) {
          triangleEdges.add((src, dst))
          commons.foreach { node =>
            triangleEdges.add((dst, node))
            triangleEdges.add(src, node)
          }
        }
      }
    }
    val numTriangleEdges = triangleEdges.size
    val groupLCC = numTriangleEdges.toFloat / ((numNodes * (numNodes-1)) / 2.0f)
    neighborTable.clear()
    (groupLCC, numTriangleEdges, numNodes)
  }

  // create neighbor table for un-directional graph, edges are already arranged as src < dst
  def getNeighborTable(edges: Iterable[(Long, Long)]): (Long2ObjectOpenHashMap[Array[Long]], Int) = {
    val neighborTable = new Long2ObjectOpenHashMap[mutable.HashSet[Long]]()
    val nodeSet = new mutable.HashSet[Long]()
    edges.foreach { case (src, dst) =>
      nodeSet.add(src)
      nodeSet.add(dst)
      val data = neighborTable.get(src)
      if (data == null) {
        val tmp = new mutable.HashSet[Long]()
        tmp.add(dst)
        neighborTable.put(src, tmp)
      } else {
        data.add(dst)
      }
    }
    val re = new Long2ObjectOpenHashMap[Array[Long]](neighborTable.size())
    neighborTable.keySet().toLongArray().foreach { src =>
      val tmp = neighborTable.get(src).toArray.sorted
      re.put(src, tmp)
    }
    neighborTable.clear()
    (re, nodeSet.size)
  }
}
