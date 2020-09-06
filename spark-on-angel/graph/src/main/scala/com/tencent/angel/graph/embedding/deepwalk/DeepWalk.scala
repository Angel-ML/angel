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

package com.tencent.angel.graph.embedding.deepwalk

import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.graph.utils.params._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

class DeepWalk(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasMaxIteration
  with HasBatchSize with HasArrayBoundsPath with HasIsWeighted with HasWeightCol with HasUseBalancePartition
  with HasNeedReplicaEdge with HasUseEdgeBalancePartition with HasWalkLength {

  def this() = this(Identifiable.randomUID("DeepWalk"))

  override def transform(dataset: Dataset[_]): DataFrame = {

    //create origin edges RDD and data preprocessing
    val rawEdges = if ($(isWeighted)) {
      dataset.select($(srcNodeIdCol), $(dstNodeIdCol), $(weightCol)).rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getLong(0), row.getLong(1), row.getFloat(2)))
        .filter(f => (f._1 != f._2) && (!f._1.isNaN) && (!f._2.isNaN) && (!f._3.isNaN))
    } else {
      dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
        .filter(row => !row.anyNull)
        .map(row => (row.getLong(0), row.getLong(1), 1f))
        .filter(f => (f._1 != f._2) && (!f._1.isNaN) && (!f._2.isNaN))
    }
    //persist the rawEdges RDD in disk
    rawEdges.persist(StorageLevel.DISK_ONLY)

    val maxId = rawEdges.flatMap(f => Iterator(f._1, f._2)).max() + 1
    val minId = rawEdges.flatMap(f => Iterator(f._1, f._2)).min()
    val numEdges = rawEdges.count()
    println(s"minId=$minId maxId=$maxId numEdges=$numEdges level=${$(storageLevel)}")

    //create final edges RDD by the $(needReplicaEdge) param
    val edges = if ($(needReplicaEdge))
      rawEdges.flatMap { case (src, dst, w) => Iterator((src, (dst, w)), (dst, (src, w))) }
    else
      rawEdges.map { case (src, dst, w) => (src, (dst, w)) }

    // calc alias table for each node
    val aliasTable = edges.groupByKey($(partitionNum)).map(x => (x._1, x._2.toArray.distinct))
      .mapPartitionsWithIndex { case (partId, iter) =>
        DeepWalk.calcAliasTable(partId, iter)
      }

    //ps process;create ps nodes adjacency matrix
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())
    val data = edges.map(_._2._1) // ps loadBalance by in degree
    val model = DeepWalkPSModel.fromMinMax(minId, maxId, data, $(psPartitionNum), useBalancePartition = $(useBalancePartition))

    //push node adjacency list into ps matrix;create graph with （node，sample path）
    var graph = aliasTable.mapPartitionsWithIndex((index, adjTable) =>
      Iterator(DeepWalkGraphPartition.initPSMatrixAndNodePath(model, index, adjTable, $(batchSize))))

    graph.persist($(storageLevel))
    //trigger action
    graph.foreachPartition(_ => Unit)

    //sample paths with random walk
    var curIteration = 0
    var prev = graph
    val beginTime = System.currentTimeMillis()
    do {
      val beforeSample = System.currentTimeMillis()
      curIteration += 1
      graph = prev.map(_.process(model, curIteration))
      graph.persist($(storageLevel))
      graph.count()
      prev.unpersist(true)
      prev = graph
      val sampleTime = System.currentTimeMillis() - beforeSample
      println(s"iter $curIteration, sampleTime: $sampleTime")
    } while (curIteration < $(walkLength) - 1)


    val EndTime = System.currentTimeMillis() - beginTime
    println(s"DeepWalkWithWeight all sampleTime: $EndTime")

    val temp = graph.flatMap(_.save())
    println(s"num path: ${temp.count()}")
    println(s"num invalid path: ${temp.filter(_.length != ${walkLength}).count()}")
    dataset.sparkSession.createDataFrame(temp.map(x => Row(x.mkString(" "))), transformSchema(dataset.schema))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(StructField("path", StringType, nullable = false)))
  }


  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}

object DeepWalk {
  def calcAliasTable(partId: Int, iter: Iterator[(Long, Array[(Long, Float)])]): Iterator[(Long, Array[Long], Array[Float], Array[Int])] = {
    iter.map { case (src, neighbors) =>
      val (events, weights) = neighbors.unzip
      val weightsSum = weights.sum
      val len = weights.length
      val areaRatio = weights.map(_ / weightsSum * len)
      val (accept, alias) = createAliasTable(areaRatio)
      (src, events, accept, alias)
    }
  }

  def createAliasTable(areaRatio: Array[Float]): (Array[Float], Array[Int]) = {
    val len = areaRatio.length
    val small = ArrayBuffer[Int]()
    val large = ArrayBuffer[Int]()
    val accept = Array.fill(len)(0f)
    val alias = Array.fill(len)(0)

    for (idx <- areaRatio.indices) {
      if (areaRatio(idx) < 1.0) small.append(idx) else large.append(idx)
    }
    while (small.nonEmpty && large.nonEmpty) {
      val smallIndex = small.remove(small.size - 1)
      val largeIndex = large.remove(large.size - 1)
      accept(smallIndex) = areaRatio(smallIndex)
      alias(smallIndex) = largeIndex
      areaRatio(largeIndex) = areaRatio(largeIndex) - (1 - areaRatio(smallIndex))
      if (areaRatio(largeIndex) < 1.0) small.append(largeIndex) else large.append(largeIndex)
    }
    while (small.nonEmpty) {
      val smallIndex = small.remove(small.size - 1)
      accept(smallIndex) = 1
    }

    while (large.nonEmpty) {
      val largeIndex = large.remove(large.size - 1)
      accept(largeIndex) = 1
    }
    (accept, alias)
  }

}
