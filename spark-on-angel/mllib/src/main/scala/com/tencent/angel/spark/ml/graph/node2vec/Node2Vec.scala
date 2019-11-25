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
package com.tencent.angel.spark.ml.graph.node2vec


import com.tencent.angel.graph.client.node2vec.PartitionHasher
import com.tencent.angel.graph.client.node2vec.data.WalkPath
import com.tencent.angel.graph.client.node2vec.updatefuncs.initwalkpath.{InitWalkPath, InitWalkPathParam}
import com.tencent.angel.graph.client.node2vec.updatefuncs.pushneighbor.{PushNeighbor, PushNeighborParam}
import com.tencent.angel.ml.matrix.{MatrixContext, RowType}
import com.tencent.angel.ps.storage.vector.element.LongArrayElement
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.ml.graph.data.NeighborTablePartition
import com.tencent.angel.spark.models.PSMatrix
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.util.Random

class Node2Vec(override val uid: String) extends Estimator[Node2VecModel]
  with Node2VecParams with DefaultParamsWritable with Logging {
  val storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK

  def this() {
    this(s"Node2Vec_${(new Random).nextInt()}")
  }

  private var neighbor: PSMatrix = _
  private var walkPath: PSMatrix = _
  private var maxNodeId: Long = -1

  private def createNeighborTableRDD(dataset: Dataset[_],
                                     srcNodeIdCol: String,
                                     dstNodeIdCol: String,
                                     partitionNum: Int,
                                     needEdgeReplica: Boolean): RDD[NeighborTablePartition[Long]] = {
    val nonEmpty = dataset.select(srcNodeIdCol, dstNodeIdCol).rdd.filter {
      case Row(src, dst) if src != null && dst != null => true
      case _ => false
    }

    val replicaDataset = if (needEdgeReplica) {
      nonEmpty.flatMap { case Row(src: Long, dst: Long) => Array((src, dst), (dst, src)) }
    } else {
      nonEmpty.map { case Row(src: Long, dst: Long) => (src, dst) }
    }

    replicaDataset.groupByKey(partitionNum).mapPartitionsWithIndex { (idx, iter) =>
      val srcIds = new ArrayBuffer[Long]()
      val neighbors = new ArrayBuffer[Array[Long]]()
      iter.foreach {
        case (src, distIter) =>
          srcIds.append(src)
          val neighbor = distIter.filter(_ != src).toSet.toArray
          neighbors.append(neighbor)
      }

      Iterator.single(new NeighborTablePartition[Long](false, idx, srcIds.toArray, neighbors.toArray, null))
    }
  }

  private def createAndPushNeighborTable[ED: ClassTag](neighborTableName: String, data: RDD[NeighborTablePartition[ED]]): Unit = {
    // Neighbor table : a (1, maxIndex + 1) dimension matrix
    val mc: MatrixContext = new MatrixContext()
    mc.setName(neighborTableName)
    mc.setRowType(RowType.T_ANY_LONGKEY_SPARSE)
    mc.setRowNum(1)
    mc.setColNum(maxNodeId)
    mc.setMaxColNumInBlock((maxNodeId + $(psPartitionNum) - 1) / $(psPartitionNum))
    mc.setValueType(classOf[LongArrayElement])
    neighbor = PSMatrix.matrix(mc)

    data.foreachPartition { iter =>
      //      PSAgentContext.get()
      //      PSContext.instance()
      val part = iter.next()
      val size = part.size
      var pos = 0
      while (pos < size) {
        val batch = part.takeBatch(pos, $(batchSize))
        val nodeIdToNeighbors = new Long2ObjectOpenHashMap[Array[Long]](batch.length)
        batch.foreach { item =>
          require(item.srcId < maxNodeId, s"${item.srcId} exceeds the maximal node index $maxNodeId")
          nodeIdToNeighbors.put(item.srcId, item.neighborIds)
        }
        val func = new PushNeighbor(new PushNeighborParam(neighbor.id, nodeIdToNeighbors))
        neighbor.asyncPsfUpdate(func).get()
        nodeIdToNeighbors.clear()
        pos += $(batchSize)
      }
    }
  }

  private def createAndInitWalkPath(name: String, walkLength: Int, mod: Int): Unit = {
    val mc: MatrixContext = new MatrixContext()
    mc.setName(name)
    mc.setRowType(RowType.T_ANY_LONGKEY_SPARSE)
    mc.setRowNum(1)
    mc.setColNum(maxNodeId)
    mc.setMaxColNumInBlock((maxNodeId + $(psPartitionNum) - 1) / $(psPartitionNum))
    mc.setValueType(classOf[WalkPath])
    walkPath = PSMatrix.matrix(mc)

    val func = new InitWalkPath(new InitWalkPathParam(walkPath.id, neighbor.id, walkLength, mod))
    walkPath.asyncPsfUpdate(func).get()
  }

  private def sample(neighborTableRDD: RDD[NeighborTablePartition[Long]]): Unit = {
    val aliasUtils = new AliasUtils(neighbor)
    val numPartition = neighborTableRDD.getNumPartitions
    aliasUtils.createAliasTable(neighborTableRDD, $(degreeBinSize), $(hitRatio)).foreachPartition { iter =>
      // PSContext.instance()
      val aliasMap = iter.next()
      val keyExample = aliasMap.keysIterator.next()
      val workPartId = PartitionHasher.getHash(keyExample._1, keyExample._2, numPartition)
      val puller = new TailPuller(walkPath, workPartId, $(pullBatchSize))
      val pusher = new TailPusher(walkPath)
      val pipelineWalker = new AliasWalker(aliasMap, neighbor, walkPath, puller, pusher, $(pValue), $(qValue))

      try {
        puller.start()
        pusher.start()
        while (pipelineWalker.hasNext) {
          pipelineWalker.next()
        }
      } finally {
        puller.shutDown()
        pusher.shutDown()
      }
    }
  }

  private def sample(spark: SparkSession, numPartition: Int): Unit = {
    val rdd = spark.sparkContext.parallelize(0 until numPartition, numPartition)

    rdd.foreachPartition { iter =>
      // PSContext.instance()
      val workPartId = iter.next()
      val puller = new TailPuller(walkPath, workPartId, $(pullBatchSize))
      val pusher = new TailPusher(walkPath)
      // JITAliasWalker
      val pipelineWalker = new RejectSamplingWalker(neighbor, walkPath, puller, pusher,
        $(pValue), $(qValue), $(degreeBinSize), $(hitRatio))

      try {
        puller.start()
        pusher.start()
        while (pipelineWalker.hasNext) {
          pipelineWalker.next()
        }
      } finally {
        puller.shutDown()
        pusher.shutDown()
      }
    }
  }

  override def fit(dataset: Dataset[_]): Node2VecModel = {
    val neighborTableRDD = createNeighborTableRDD(dataset, $(srcNodeIdCol), $(dstNodeIdCol), $(partitionNum), $(needReplicaEdge))
      .persist(storageLevel)

    maxNodeId = neighborTableRDD.mapPartitions { iter =>
      val partNeigh = iter.next()
      if (partNeigh == null || partNeigh.srcIds.isEmpty) {
        Iterator.single(-1L)
      } else {
        val maxIdx = partNeigh.srcIds.max
        Iterator.single(maxIdx)
      }
    }.max() + 1L

    println("begin to push NeighborTable ...")
    var start = System.currentTimeMillis()
    createAndPushNeighborTable("NeighborTable", neighborTableRDD)
    var end = System.currentTimeMillis()
    println(s"finish to push NeighborTable, time: ${(end - start) / 1000.0}")
    neighborTableRDD.unpersist(true)

    println("begin to create and init WalkPath ...")
    start = System.currentTimeMillis()
    createAndInitWalkPath("WalkPath", $(walkLength), $(partitionNum))
    end = System.currentTimeMillis()
    println(s"finish to create and init WalkPath, time: ${(end - start) / 1000.0}")

    println("start to sample path ...")
    start = System.currentTimeMillis()
    sample(dataset.sparkSession, $(partitionNum))
    // sample(neighborTableRDD)
    end = System.currentTimeMillis()
    println(s"finish to sample path, time: ${(end - start) / 1000.0}")

    val model = copyValues(new Node2VecModel(uid, walkPath))
    model.setMaxNodeId(maxNodeId)

    model
  }

  override def copy(extra: ParamMap): Estimator[Node2VecModel] = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = schema

}


object Node2Vec extends DefaultParamsReadable[Node2Vec] {

  override def load(path: String): Node2Vec = super.load(path)

  def startPS(sc: SparkContext): Unit = {
    PSContext.getOrCreate(sc)
  }

  def stopPS(): Unit = {
    PSContext.stop()
  }
}
