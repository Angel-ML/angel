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
package com.tencent.angel.graph.community.louvain

import com.tencent.angel.ml.math2.vector.LongFloatVector
import com.tencent.angel.graph.community.louvain.LouvainGraph.edgeTripleRDD2GraphPartitions
import com.tencent.angel.graph.community.louvain.LouvainPSModel
import org.apache.spark.graphx.PartitionStrategy.EdgePartition2D
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.HashPartitioner

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.graphx.{Edge, Graph, VertexId}

/**
 * An implement of louvain algorithm(known as fast unfolding)
 *
 */

object LouvainGraph {

  /**
   * transform edges rdd to louvain graph partition
   *
   * @param tripleRdd    the edges triple RDD
   * @param model        louvain ps model
   * @param numPartition the rdd partition number
   * @param storageLevel
   * @return
   */
  def edgeTripleRDD2GraphPartitions(tripleRdd: RDD[(Long, Long, Float)],
                                    model: LouvainPSModel = null,
                                    numPartition: Option[Int] = None,
                                    storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY)
  : RDD[LouvainGraphPartition] = {

    val partNum = numPartition.getOrElse(tripleRdd.getNumPartitions)
    tripleRdd.flatMap { case (src, dst, wgt) =>
      Iterator((src, (dst, wgt)), (dst, (src, wgt)))
    }.groupByKey(partNum).mapPartitions { iter =>
      if (iter.nonEmpty) {
        val keys = new ArrayBuffer[Long]()
        val neighbors = new ArrayBuffer[Array[Long]]()
        val weights = new ArrayBuffer[Array[Float]]()
        iter.foreach { case (key, group) =>
          keys += key
          val (e, w) = group.unzip
          neighbors += e.toArray
          weights += w.toArray
        }
        Iterator.single((keys.toArray, neighbors.toArray, weights.toArray))
      } else {
        Iterator.empty
      }
    }.map { case (keys, neighbors, weights) =>
      // calc nodeWeights
      val nodeWeights = if (null != model) {
        model.getCommInfo(keys).get(keys)
      } else {
        weights.map(_.sum)
      }
      new LouvainGraphPartition(keys, neighbors, weights, nodeWeights)
    }.persist(storageLevel)
  }
}


class LouvainGraph(
                    @transient val graph: RDD[LouvainGraphPartition],
                    louvainPSModel: LouvainPSModel) extends Serializable {

  //2m for unweighted graph ; 2*sum(edge weights) for weighted graph
  private lazy val totalWeights: Double = {
    val total = graph.map(_.partitionWeights).sum()
    println(s"totalNodeWeights=$total")
    total
  }


  /**
   * set community id as the the minimum id of nodes in it
   *
   * @param model      louvain ps model
   * @param bufferSize the batch size to operate in ps
   */
  def correctCommunityId(model: LouvainPSModel, bufferSize: Int = 1000000): Unit = {
    val commInfoDeltaRDD = commId2NodeId(model).groupByKey.mapPartitions { iter =>
      if (iter.nonEmpty) {
        val oldCommIdsBuffer = new ArrayBuffer[Long]()
        val newCommIdsBuffer = new ArrayBuffer[Long]()

        val nodesBuffer = new ArrayBuffer[Long](bufferSize)
        val nodeNewCommIdsBuffer = new ArrayBuffer[Long](bufferSize)
        var numNodeCorrected = 0
        while (iter.hasNext) {
          var used = 0
          while (iter.hasNext && used < bufferSize) {
            val (commId, nodes) = iter.next
            val newId = nodes.min

            oldCommIdsBuffer += commId
            newCommIdsBuffer += newId

            nodesBuffer ++= nodes
            nodeNewCommIdsBuffer ++= Array.tabulate(nodes.size)(_ => newId)

            used += nodes.size
            numNodeCorrected += 1
          }
          if (used > 0) {
            // update node2comm
            println(s"numNodeCorrected=$numNodeCorrected")
            model.updateNode2community(nodesBuffer.toArray, nodeNewCommIdsBuffer.toArray)
            nodesBuffer.clear()
            nodeNewCommIdsBuffer.clear()
          }
        }

        val oldCommIds = oldCommIdsBuffer.toArray
        val oldInfo = model.getCommInfo(oldCommIds).get(oldCommIds)
        Iterator((oldCommIds, oldInfo.map(x => -x)), (newCommIdsBuffer.toArray, oldInfo))
      } else {
        Iterator.empty
      }
    }.cache()

    // since we calculate delta from old community info from ps,
    // the old community info should preserve before all task has finished.
    // here we use a handy trigger job for this purpose.
    commInfoDeltaRDD.foreachPartition(_ => Unit)

    // update community info
    commInfoDeltaRDD.foreach { case (commIds, commInfoDelta) =>
      model.incrementCommWeight(commIds, commInfoDelta)
    }
    commInfoDeltaRDD.unpersist(false)
  }

  /**
   * get the pair(community,node) in all partitions
   *
   * @param model louvain ps model
   * @return
   */
  def commId2NodeId(model: LouvainPSModel): RDD[(Long, Long)] = {
    graph.flatMap(_.partComm2nodeParis(model))
  }

  /**
   * check the community id is the min of node ids
   *
   * @param model louvain ps model
   * @return the wrong nodes number
   */
  def checkCommId(model: LouvainPSModel): Long = {
    // check ids
    commId2NodeId(model).groupByKey.filter { case (commId, nodes) =>
      commId != nodes.min
    }.count()
  }

  /**
   * comuter the total weights for check
   *
   * @param model louvain ps model
   * @return
   */
  def checkTotalSum(model: LouvainPSModel): Double = {
    // check totalSum
    distinctCommIds(model).mapPartitions { iter =>
      if (iter.nonEmpty) {
        val arr = iter.toArray
        val s = model.community2weightPSVector.pull(arr).asInstanceOf[LongFloatVector].sum()
        Iterator(s)
      } else {
        Iterator(0.0)
      }
    }.sum()
  }

  /**
   * get all distinct community ids
   *
   * @param model louvain ps model
   * @return
   */
  def distinctCommIds(model: LouvainPSModel): RDD[Long] = {
    graph.flatMap(_.partCommunityIds(model)).distinct()
  }

  /**
   * save (node id ,community id) in path
   *
   * @param path     save path
   * @param nodesRDD node list
   */
  def save(path: String, nodesRDD: RDD[Long]): Unit = {
    nodesRDD.mapPartitions { iter =>
      val nodes = iter.toArray
      val pair = nodes.zip(louvainPSModel.getNode2commMap(nodes).get(nodes))
      pair.map { case (id, comm) => s"$id,$comm" }.toIterator
    }.saveAsTextFile(path)
  }

  /**
   * update node weights and community weights for each partition
   *
   * @return
   */
  def updateNodeWeightsToPS(): this.type = {
    graph.foreach(_.updateNodeWeightsToPS(louvainPSModel))
    this
  }


  /**
   * the first phase of the louvain algorithm
   *
   * @param maxIter   the max iteration number in first phase
   * @param batchSize the batch size for compute
   * @param eps       the min modularity increment value
   */
  def modularityOptimize(maxIter: Int, batchSize: Int, eps: Double = 0.0, preserveRate: Double = 0.0, useMergeStrategy: Boolean): Unit = {
    var curTime = System.currentTimeMillis()
    val q2 = Q2()
    val q1 = Q1()
    var deltaQ = Double.MaxValue
    var prevQ = q1 - q2
    println(s"Q1=$q1, Q2=$q2, Q = ${q1 - q2}, takes ${System.currentTimeMillis() - curTime}ms")
    var optIter = 0
    while (optIter < maxIter && deltaQ > eps) {
      curTime = System.currentTimeMillis()
      modularityOptimize(batchSize, shuffle = optIter != 0, preserveRate)
      println(s"opt, takes ${System.currentTimeMillis() - curTime}ms")

      curTime = System.currentTimeMillis()
      val q1 = Q1()
      val q2 = Q2()
      val q = q1 - q2
      println(s"Q1=$q1, Q2=$q2, Q = $q, takes ${System.currentTimeMillis() - curTime}ms")
      curTime = System.currentTimeMillis()
      deltaQ = q - prevQ
      prevQ = q
      println(s"numCommunity = $getNumOfCommunity, takes ${System.currentTimeMillis() - curTime}ms")
      if (useMergeStrategy) {
        mergeCommunity(batchSize, checkpointInterval = 50)
      }

      optIter += 1
    }
  }

  private def Q2(): Double = {
    louvainPSModel.sumOfSquareOfCommunityWeights / math.pow(totalWeights, 2)
  }

  private def modularityOptimize(batchSize: Int, shuffle: Boolean, preserveRate: Double): Unit = {
    graph.foreach(_.modularityOptimize(louvainPSModel, totalWeights, batchSize, shuffle, preserveRate))
  }

  private def Q1(): Double = {
    1 - sumOfWeightsBetweenCommunity / totalWeights
  }

  def getModularity(): Double = {
    Q1() - Q2()
  }

  /**
   * compute the weight between two communities
   *
   * @return
   */
  private def sumOfWeightsBetweenCommunity: Double = {
    this.graph.map(_.sumOfWeightsBetweenCommunity(louvainPSModel)).sum()
  }

  /**
   * get the communities number
   *
   * @return
   */
  def getNumOfCommunity: Long = {
    this.graph.flatMap(_.node2community(louvainPSModel)).values.distinct().count()
  }

  /**
   * the second phase of louvain;build a new network of communities
   *
   * @param batchSize the compute size
   * @param storageLevel
   * @return
   */
  def folding(batchSize: Int, storageLevel: StorageLevel): LouvainGraph = {
    val curTime = System.currentTimeMillis()
    val newEdges = this.graph.flatMap { part =>
      part.partFolding(louvainPSModel, batchSize)
    }.reduceByKey(_ + _).map { case ((src, dst), wgt) =>
      (src, dst, wgt / 2.0f)
    }
    val newGraph = edgeTripleRDD2GraphPartitions(newEdges, louvainPSModel)
    newGraph.foreachPartition(_ => Unit)
    this.graph.unpersist()
    println(s"folding, takes ${System.currentTimeMillis() - curTime}ms")
    new LouvainGraph(newGraph, louvainPSModel)
  }

  private def hist: (Array[Double], Array[Long]) = {
    this.graph.flatMap(_.node2community(louvainPSModel)).values.histogram(25)
  }


  /**
   * build a new graph with edge(cpmmunityId,nodeId);Compute the connected component
   *
   * @param checkpointInterval
   * @return
   */
  def mergeCommunity(bufferSize: Int = 1000000, checkpointInterval: Int): Unit = {
    val oriRdd = commId2NodeId(louvainPSModel).map(_.swap)

    val DUMMY_VALUE = null.asInstanceOf[Byte]
    val partitionNum = oriRdd.getNumPartitions
    val edgeRdd = oriRdd.map {
      case (src, dst) =>
        val pid = EdgePartition2D.getPartition(src, dst, partitionNum)
        if (src < dst) (pid, (src, dst)) else (pid, (dst, src))
    }.partitionBy(new HashPartitioner(partitionNum))
      .map { case (_, (src, dst)) => new Edge(src, dst, DUMMY_VALUE) }

    val rawG = Graph.fromEdges(edgeRdd, 1L)
      .mapVertices { case (vid, _) => vid.toLong }
      .persist()

    var messages = rawG.aggregateMessages[Long](
      {
        t =>
          if (t.srcAttr < t.dstAttr)
            t.sendToSrc(t.dstAttr)
          if (t.dstAttr < t.srcAttr)
            t.sendToDst(t.srcAttr)
      }, math.max
    ).persist()
    var activeMessages = messages.count()
    var i = 0
    var g = rawG
    val maxiter = 30
    while (i < maxiter && activeMessages > 0) {
      println(s"+++ merge community iter: $i, activeMsg: $activeMessages")

      i = i + 1
      val prevG = g

      g = g.outerJoinVertices[Long, Long](messages) {
        case (_, data, Some(update)) => Math.max(data, update)
        case (_, data, None) => data
      }.persist()
      if (i % checkpointInterval == 0) {
        g.checkpoint()
      }
      val oldMessage = messages
      messages = g.aggregateMessages[Long](
        {
          t =>
            if (t.srcAttr < t.dstAttr)
              t.sendToSrc(t.dstAttr)
            if (t.dstAttr < t.srcAttr)
              t.sendToDst(t.srcAttr)
        }, math.max
      ).persist()
      activeMessages = messages.count()
      prevG.unpersistVertices()
      prevG.edges.unpersist()
      oldMessage.unpersist()
    }
    messages.unpersist()

    val result = g.vertices.map(e => (e._1, e._2)).persist()

    result.count()
    g.unpersistVertices()
    g.edges.unpersist()


    //result.sortBy(_._1).foreach(println)
    val old2NewCommRDD = oriRdd.leftOuterJoin(result).map {
      case (vId, (oldComm, newCommOpt)) => (vId, oldComm, newCommOpt.getOrElse(oldComm))
    }.mapPartitions { iter =>
      if (iter.nonEmpty) {
        val oldCommIdsBufferAll = new ArrayBuffer[Long]()
        val newCommIdsBuffer = new ArrayBuffer[Long]()
        val newCommIdsBufferAll = new ArrayBuffer[Long]()
        val nodesBuffer = new ArrayBuffer[Long]()

        while (iter.hasNext) {
          var used = 0
          while (iter.hasNext && used < bufferSize) {
            val (vId, oldComm, newComm) = iter.next
            if (oldComm != newComm) {
              oldCommIdsBufferAll += oldComm
              newCommIdsBufferAll += newComm
              newCommIdsBuffer += newComm
              nodesBuffer += vId
              used += 1
            }
          }
          if (used > 0) {
            // update node2comm
            louvainPSModel.updateNode2community(nodesBuffer.toArray, newCommIdsBuffer.toArray)
            nodesBuffer.clear()
            newCommIdsBuffer.clear()
          }
        }

        val oldCommIdsTmp = oldCommIdsBufferAll.toArray
        val newCommIdsTmp = newCommIdsBufferAll.toArray
        oldCommIdsTmp.zip(newCommIdsTmp).distinct.toIterator
      }
      else {
        Iterator.empty
      }
    }

    old2NewCommRDD.distinct().foreachPartition { iter =>
      if (iter.nonEmpty) {
        val (oldCommIds, newCommIds) = iter.toArray.unzip
        val oldInfo = louvainPSModel.getCommInfo(oldCommIds).get(oldCommIds)

        louvainPSModel.incrementCommWeight(oldCommIds, oldInfo.map(x => -x))

        val newCommIds2info = newCommIds.zip(oldInfo).groupBy(_._1).map { a =>
          (a._1, a._2.unzip._2.sum)
        }.toArray.unzip

        louvainPSModel.incrementCommWeight(newCommIds2info._1, newCommIds2info._2)

      }
    }
  }
}


