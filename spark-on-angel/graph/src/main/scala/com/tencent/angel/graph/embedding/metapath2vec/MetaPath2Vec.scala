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

package com.tencent.angel.graph.embedding.metapath2vec

import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.graph.utils.params._
import com.tencent.angel.graph.utils.partitionstrategy.reindexpartition.IndexPartitioner
import com.tencent.angel.graph.utils.BatchIter
import com.tencent.angel.graph.psf.neighbors.samplebyaliastable.initaliastable.{InitNeighborAliasTable, InitNeighborAliasTableParam}
import com.tencent.angel.graph.psf.neighbors.samplebyaliastable.samplealiastable.{GetNeighborAliasTable, GetNeighborAliasTableParam, GetNeighborAliasTableResult, NeighborsAliasTableElement}
import com.tencent.angel.spark.ml.util.LoadBalancePartitioner
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class MetaPath2Vec(override val uid: String) extends Transformer
  with HasSrcNodeIdCol with HasDstNodeIdCol with HasOutputNodeIdCol with HasOutputCoreIdCol
  with HasStorageLevel with HasPartitionNum with HasPSPartitionNum with HasUseBalancePartition
  with HasBalancePartitionPercent with HasBatchSize with HasIsWeighted with HasWeightCol
  with HasWalkLength with HasNeedReplicaEdge with HasPullBatchSize with HasEpochNum {

  def this() = this(Identifiable.randomUID("metaPath2vec"))

  private var metaPathMap: mutable.Set[(Int, Int)] = _
  private var nodeAttr: RDD[(Long, Int)] = _
  private var psMatrix: PSMatrix = _

  def setMetaPath(meta: mutable.Set[(Int, Int)]): Unit = {
    metaPathMap = meta
  }

  def setNodeAttr(attr: RDD[(Long, Int)]): Unit = {
    nodeAttr = attr
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val rawEdges = if (${isWeighted}) {
      dataset.select($(srcNodeIdCol), $(dstNodeIdCol), ${weightCol}).rdd
        .map(row => (row.getLong(0), (row.getLong(1), row.getFloat(2))))
        .filter(f => f._1 != f._2._1)
    } else {
      dataset.select($(srcNodeIdCol), $(dstNodeIdCol)).rdd
        .map(row => (row.getLong(0), (row.getLong(1), 1f)))
        .filter(f => f._1 != f._2._1)
    }
    rawEdges.persist(StorageLevel.DISK_ONLY)

    val node = rawEdges.flatMap(x => Iterator(x._1, x._2._1))
    val maxId = node.max() + 1
    val minId = node.min()

    // join src and dst with node type, filter out edges that are consistent with the given metaPath
    val edges = if (nodeAttr == null) {
      println(s"no nodeType input, set all node to type 0.")
      if (${needReplicaEdge})
        rawEdges.flatMap{ case (src, (dst, w)) => Iterator((src,(dst,w)), (dst, (src, w)))}
      else rawEdges
    } else {
      val temp = rawEdges.leftOuterJoin(nodeAttr).map { case (src, ((dst, w), attr)) => (dst, (attr.get, src, w))}
        .leftOuterJoin(nodeAttr).map { case (dst, ((srcType, src, w), attr)) => (src, srcType, dst, attr.get, w)}
      MetaPath2VecV1.filterEdgesByMetaPath(temp, ${needReplicaEdge}, metaPathMap)
    }
    edges.persist(StorageLevel.DISK_ONLY)
    rawEdges.unpersist()

    // calc alias table for each node
    val aliasTable = edges.groupByKey(${partitionNum}).map(x => (x._1, x._2.toArray.distinct))
      .mapPartitionsWithIndex { case (partId, iter) =>
        MetaPath2VecV1.calcAliasTable(partId, iter)
      }.persist(StorageLevel.DISK_ONLY)

    // push aliasTable to ps
    // Start PS and init the model
    println("start to run ps")
    PSContext.getOrCreate(SparkContext.getOrCreate())
    val data = edges.map(_._2._1) // ps loadBalance by in degree
    initAliasTableToPS(data, aliasTable, maxId, minId, ${psPartitionNum},
      useBalancePartition = ${useBalancePartition}, pushBatchSize = ${batchSize})

    val numReplicatedEdges = edges.count()
    println(s"minId=$minId maxId=$maxId numEdges(replicated ?)=$numReplicatedEdges")

    // psMatrix checkpoint
    psMatrix.checkpoint()
    aliasTable.unpersist()

    val psPartitions = PSAgentContext.get.getMatrixMetaManager.getPartitions(psMatrix.id)
    var j = 0
    val psPartitionsRange = new ArrayBuffer[(Long, Long)]()
    while (j < psPartitions.size()) {
      val partitionKey = psPartitions.get(j)
      psPartitionsRange += ((partitionKey.getStartCol, partitionKey.getEndCol))
      j += 1
    }

    val arrayBounds = psPartitionsRange.map(_._2 - 1).slice(0, psPartitionsRange.length - 1).toArray
    val psPartNum = arrayBounds.length + 1
    println(s"psPartitionsSize: ${psPartitions.size()}, IndexPartitioner's psPartNum: $psPartNum")
    val partitioner = new IndexPartitioner(psPartNum, arrayBounds)

    // first step sampling
    var result = aliasTable.mapPartitions { iter =>
      iter.map { case (src, neighbors, accept, alias) =>
        val bucket = Random.nextInt(neighbors.length)
        val shouldAccept = Random.nextFloat()
        val sampled = if (shouldAccept < accept(bucket)) neighbors(bucket) else neighbors(alias(bucket))
        (sampled, Array(src, sampled))
      }
    }.persist(${storageLevel})
    // next steps

    result.count()
    var tempResult = result
    var input = result.map(x => (x._1, 1)).reduceByKey(partitioner, _ + _).persist(${storageLevel})
    input.count()
    var i = 2
    var tempInput = input
    while (i < ${walkLength}) {
      val before = System.currentTimeMillis()
      val pathRe = tempInput.mapPartitionsWithIndex { case (index, iter) =>
        var sampleTime = 0L
        BatchIter(iter, ${pullBatchSize}).flatMap { batchIter =>
          val (nodes, count) = batchIter.unzip
          val beforeSample = System.currentTimeMillis()
          val sampled = MetaPath2VecV1.getSampledNeighbors(psMatrix, nodes, count)
          sampleTime += (System.currentTimeMillis() - beforeSample)
          println(s"partition $index, iter $i, sampleTime: $sampleTime")
          nodes.map { node => (node, sampled.get(node))}
        }
      }
      result = tempResult.join(pathRe, partitioner).map { case (key, (path, sampled)) =>
        val next = sampled(Random.nextInt(sampled.length))
        (next, path ++ Array(next))
      }.persist(${storageLevel})
      input = result.map(x => (x._1, 1)).reduceByKey(partitioner, _ + _).persist(${storageLevel})
      input.count()
      tempInput.unpersist()
      tempInput = input
      tempResult.unpersist()
      tempResult = result
      println(s"finished $i th sampling, cost ${System.currentTimeMillis() - before} ms.")
      i += 1
    }

    println(s"num path: ${result.count()}")
    println(s"num invalid path: ${result.filter(_._2.length != ${walkLength}).count()}")

    // save result
    dataset.sparkSession.createDataFrame(result.map(x => Row(x._2.mkString(" "))), transformSchema(dataset.schema))
  }

  def initAliasTableToPS(data: RDD[Long], aliasTable: RDD[(Long, Array[Long], Array[Float], Array[Int])], maxId: Long, minId: Long = 0,
                         psPartitionNum: Int,
                         useBalancePartition: Boolean = false,
                         pushBatchSize: Int = 10000,
                         tableName: String = "aliasTable"): Long = {
    val mc: MatrixContext = new MatrixContext()
    mc.setName(tableName)
    mc.setRowType(RowType.T_ANY_LONGKEY_SPARSE)
    mc.setRowNum(1)
    mc.setIndexStart(minId)
    mc.setIndexEnd(maxId)
    mc.setColNum(maxId - minId)
    mc.setMaxColNumInBlock((maxId - minId) / psPartitionNum)
    mc.setMaxColNumInBlock(maxId / psPartitionNum)

    mc.setValueType(classOf[NeighborsAliasTableElement])

    if (useBalancePartition) {
      LoadBalancePartitioner.partition(data, maxId, psPartitionNum, mc, minId = minId)
    }

    psMatrix = PSMatrix.matrix(mc)
    aliasTable.mapPartitions { iter =>
      iter.sliding(pushBatchSize, pushBatchSize).map(pairs => MetaPath2VecV1.initAliasTable(psMatrix, pairs))
    }.count()
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(StructField("path", StringType, nullable = false)))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}

object MetaPath2VecV1 {

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

  def initAliasTable(psMatrix: PSMatrix, pairs: Seq[(Long, Array[Long], Array[Float], Array[Int])]): Int = {
    val nodeId2Neighbors = new Long2ObjectOpenHashMap[NeighborsAliasTableElement](pairs.length)
    pairs.foreach { case (src, neighbors, accept, alias) =>
      val elem = new NeighborsAliasTableElement(neighbors, accept, alias)
      nodeId2Neighbors.put(src, elem)
    }
    val func = new InitNeighborAliasTable(new InitNeighborAliasTableParam(psMatrix.id, nodeId2Neighbors))
    psMatrix.asyncPsfUpdate(func).get()
    nodeId2Neighbors.clear()
    println(s"init ${pairs.length} long neighbors with  alias table.")
    pairs.length
  }

  def getSampledNeighbors(psMatrix: PSMatrix, nodeIds: Array[Long], count: Array[Int]): Long2ObjectOpenHashMap[Array[Long]] = {
    psMatrix.psfGet(new GetNeighborAliasTable(new GetNeighborAliasTableParam(psMatrix.id, nodeIds, count)))
      .asInstanceOf[GetNeighborAliasTableResult].getNodeIdToNeighbors
  }

  def filterEdgesByMetaPath(edges: RDD[(Long, Int, Long, Int, Float)], needReplicaEdge: Boolean = false,
                            metaPathMap: mutable.Set[(Int, Int)]): RDD[(Long, (Long, Float))] = {
    println(s"metaPathMap: ${metaPathMap.toArray.map{ case (a, b) => a + "->" + b }.mkString(", ")}")
    if (needReplicaEdge) {
      println(s"replicate edges...")
      edges.flatMap { case (src, srcType, dst, dstType, w) =>
        if (metaPathMap.contains((dstType, srcType)) && metaPathMap.contains((srcType, dstType))) Iterator((src, (dst, w)), (dst, (src, w)))
        else if (metaPathMap.contains((srcType, dstType))) Iterator.single(src, (dst, w))
        else if (metaPathMap.contains((dstType, srcType))) Iterator.single(dst, (src, w))
        else Iterator.empty
      }
    } else {
      edges.flatMap { case (src, srcType, dst, dstType, w) =>
        if (metaPathMap.contains((srcType, dstType))) Iterator.single(src, (dst, w))
        else Iterator.empty
      }
    }
  }
}