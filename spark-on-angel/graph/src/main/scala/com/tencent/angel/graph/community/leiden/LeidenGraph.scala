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
package com.tencent.angel.graph.community.leiden

import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap
import org.apache.spark.SparkPrivateClassProxy
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.beans.BeanProperty
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import com.tencent.angel.spark.ml.util.Utils.{runOnTimer, secondTimer}


/**
 * An implement of Leiden algorithm
 */
private[leiden] class LeidenGraph(@transient @BeanProperty var graphs: RDD[CSR], val model: LeidenPSModel) extends Serializable {

  /**
   * Create a singleton graph, in which each community consists of exactly one vertex.
   *
   * @param reset reset
   * @return
   */
  def initPSModel(reset: Boolean = false): this.type = {
    if (reset) model.resetMode()
    graphs.foreach(csr => model.update(csr.superNodes,
      csr.nodePros,
      Iterator.range(csr.min.toInt, (csr.max + 1).toInt).map(_.toLong).toArray,
      csr.tightness))
    this
  }

  def singleton(): this.type = {
    this.graphs.foreach(g => model.updateNode2community(g.superNodes, Iterator.range(g.min.toInt, (g.max + 1).toInt).map(_.toLong).toArray))
    this
  }

  def communityCount(): Long = {
    val comms = this.graphs.flatMap(g => {
      val node2comm = model.getNode2Comm(g.superNodes)
      node2comm.getStorage.getValues.map(_ -> 1)
    }).reduceByKey(_ + _)
    comms.count()
  }

  def nodeCount(): Long = {
    this.graphs.map(g => g.superNodes.length).sum().toLong
  }

  /**
   * Perform fast local node moves to communities to improve the partition's quality.
   *
   * For every node, greedily move it to a neighboring community, maximizing the improvement in the partition's quality.
   */
  def optimize(gamma: Float, optimizeTimes: Int): Unit = {
    var optimized = false
    var step = 0
    while (!optimized && step < optimizeTimes) {
      val queueCount = this.graphs.map(_.queue.size).sum().toLong
      step += 1
      val (_, mCost) = runOnTimer(() => {
        this.graphs.foreach { g =>
          val (_, bCost) = runOnTimer(() => {
            g.moveNodesOnBatch(gamma, model)
          })
          println(s"[graph:${g.id}] move nodes finished cost ${secondTimer(bCost)}s.")
        }
      })
      val (_, uCost) = runOnTimer(() => updateStatus())
      optimized = this.graphs.filter(_.queue.nonEmpty).map(_.id).count() == 0
      println(s"[optimize:$step queue:$queueCount] move nodes cost${secondTimer(mCost)}s, update queue cost ${secondTimer(uCost)}s.")
    }
  }

  /**
   * new node into queue
   *
   */
  private def updateStatus(): Unit = {
    // on graph
    this.graphs.foreach(_.refreshQueueStatus(model))
    // on ps
    model.resetRankNodeStatus
    model.resetNode2Visited
  }

  /**
   * Refine all communities by merging repeatedly, starting from a singleton partition.
   *
   * @param gamma        gamma
   * @param theta        theta
   * @param batchSize    batchSize
   * @param storageLevel storageLevel
   */
  def refineGraph(gamma: Float, theta: Float, batchSize: Int, storageLevel: StorageLevel): Unit = {
    val numPartition = this.graphs.getNumPartitions
    val batches = this.graphs.flatMap { g =>
      g.superNodes.indices.iterator.sliding(batchSize, batchSize).flatMap { batchIndex =>
        val nodes = batchIndex.flatMap(i => g.getEdges(i) ++ Array(g.superNodes(i))).toArray.distinct
        val node2comm = this.model.getNode2Comm(nodes)
        val group = batchIndex.map { i =>
            val comm = node2comm.get(g.superNodes(i))
            val node = g.superNodes(i)
            val nodeSize = g.getNodePro(i)
            val nbrs = g.getEdges(i).zip(g.getEdgePros(i)).filter(x => node2comm.get(x._1) == comm)
            comm -> ((node, nodeSize), nbrs)
          }.groupBy(_._1)
          .map { x => x._1 -> x._2.map(_._2) }
        val communities = group.keys.toArray
        val communityNodes = model.getCommNodes(communities)
        group.map { case (comm, info) => ((comm, communityNodes.get(comm)), info) }
      }
    }.reduceByKey(_ ++ _, numPartition)
    val _ = batches.count()
    model.resetMode()
    singleton()
    batches.foreachPartition { iter =>
      val items = iter.toArray
      val allNodes = items.flatMap(x => x._2.map(_._1._1))
      val node2RComm = model.getNode2Comm(allNodes)

      val itemBatch = items.map { case ((comm, commSize), group) =>
        val node2pro = group.map(_._1)
        val node2NewComm = new Long2LongOpenHashMap()
        val commNewInfo = SparkPrivateClassProxy.createOpenHashMap[Long, (Float, Float, Long)]()
        node2pro.foreach { case (id, pro) =>
          val oldComm = node2RComm.get(id)
          node2NewComm.put(id, oldComm)
          commNewInfo.changeValue(oldComm, (pro, 0.0F, oldComm), _ => (pro, 0.0F, oldComm))
        }

        //println(s"$comm:[${group.map(_._1._1).mkString(", ")}] [${group.map(x => s"[${x._2.map(y => s"${y._1}").mkString(", ")}]").mkString(", ")}]")

        // Consider only nodes that are well connected within subset S
        // R = {v | v ∈ S, E(v, S − v) ≥ γ∥v∥ · (∥S∥ − ∥v∥)}
        val R = group.filter { case ((_, srcSize), edges) => edges.map(_._2).sum >= gamma * srcSize * (commSize - srcSize) }
        //println(s"  >R:[${R.map(_._1._1).mkString(", ")}]")

        // Visit nodes (in random order)
        R.foreach { case ((node, size), _) =>
          val nodeRComm = node2NewComm.get(node)
          val newSize = commNewInfo(nodeRComm)._1

          // singleton community
          if (newSize <= size) {
            // Consider only well-connected communities
            // T ← {C | C ∈ P, C ⊆ S, E(C, S − C) ≥ γ∥C∥ · (∥S∥ − ∥C∥)}
            val T = group.map { case ((node, _), nbrs) =>
              node2NewComm.get(node) -> (node, nbrs)
            }.groupBy(_._1).filter { case (_, nbrs) =>
              val (commRNodes, pair) = nbrs.map(_._2).unzip
              // E(C, S − C)
              val edges = pair.flatMap { x => x.filter(y => !commRNodes.contains(y._1)).map(_._2) }.sum
              // γ∥C∥ · (∥S∥ − ∥C∥)}
              edges >= gamma * newSize * (commSize - newSize)
            }
            //println(s"   T:{${T.map(x => s"${x._1}:[${x._2.map(_._2._1).mkString(", ")}]").mkString(", ")}}")

            val candidates = T.filter(x => x._1 != nodeRComm).map { case (rComm, g) =>
              val edgeWeight = g.map(_._2._2)
              val src2commEdges = edgeWeight.flatten.filter(_._1 == node).map(_._2).sum
              val delta = CPM.delta(src2commEdges, 0.0F, gamma, size, commNewInfo(rComm)._1, size)
              val pr = if (delta >= 0) math.exp(delta / theta) else 0.0F
              //println(s"   commR:$rComm nodesR:[${g.map(_._2._1).mkString(", ")}] $delta=$src2commEdges-0.0F-$resolution*$size*($size+${commNewInfo(rComm)._1}-$size) pr:$pr")
              (rComm, pr, src2commEdges)
            }

            if (candidates.nonEmpty) {
              val (newRComm, _, src2commEdges) = candidates.maxBy(_._2)
              //println(s"  $node from $nodeRComm to $newRComm src2commREdges:$src2commEdges")

              commNewInfo.changeValue(nodeRComm, (0.0F, 0.0F, nodeRComm), _ => (0.0F, 0.0F, newRComm))
              commNewInfo.changeValue(newRComm, (0.0F, 0.0F, newRComm), x => (x._1 + size, x._2 + src2commEdges, x._3))

              //  move node & community merge
              node2NewComm.put(node, newRComm)
            }
          }
        }
        val commInfo = commNewInfo.map { case (oldComm, (nodeSize, edgeSize, newComm)) => ((oldComm, newComm), (nodeSize, edgeSize)) }.toArray
        val nodeInfo = node2NewComm.asScala.toArray.map(x => x._1.toLong -> x._2.toLong)
        (nodeInfo, commInfo)
      }

      val (nodeChangeInfo, commChangeInfo) = itemBatch.unzip
      val (nodes, nodeNewComms) = nodeChangeInfo.flatten.unzip
      val (commIdChange, infoChange) = commChangeInfo.flatten.unzip
      val (oldComms, newComms) = commIdChange.unzip
      val (nodeInfo, edgeInfo) = infoChange.unzip
      model.updateComm2nodes(oldComms, nodeInfo)
      model.updateComm2edges(oldComms, edgeInfo)
      model.updateOld2New(oldComms, newComms)
      model.updateNode2community(nodes, nodeNewComms)
    }
  }

  /**
   * Create an aggregate graph of the graph G.
   *
   * The aggregate graph is a multi-graph, in which the nodes of every partition set have been coalesced into a single
   * node. Every edge between two nodes a and b is represented by an edge in the multi-graph, between the nodes that
   * represent the communities that a and b, respectively, are members of.
   *
   * @param batchSize batchSize
   * @return
   */
  def aggregateGraph(batchSize: Int, storageLevel: StorageLevel): this.type = {
    val newEdges = this.graphs.flatMap(g => g.aggregateOnBatch(model))
      .reduceByKey(_ + _).map { case ((src, dst), wgt) =>
        (src, dst, wgt)
      }
    val newGraphs = LeidenGraph.fromEdges(newEdges, model, batchSize = batchSize).persist(storageLevel)
    val _ = newGraphs.count()
    this.graphs.unpersist()
    this.setGraphs(newGraphs)
    this
  }

}

object LeidenGraph {

  def fromEdges(tripleRdd: RDD[(Long, Long, Float)],
                model: LeidenPSModel = null,
                numPartition: Option[Int] = None,
                batchSize: Int)
  : RDD[CSR] = {
    val partNum = numPartition.getOrElse(tripleRdd.getNumPartitions)
    val tmp = tripleRdd.flatMap { case (src, dst, wgt) =>
        Iterator((src, (dst, wgt)), (dst, (src, wgt)))
      }.groupByKey(partNum).mapPartitionsWithIndex { (i, iter) =>
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
          Iterator.single((i, keys.toArray, neighbors.toArray, weights.toArray))
        } else {
          Iterator.empty
        }
      }.filter(_._2.nonEmpty)
      .map { case (i, keys, neighbors, weights) =>
        // calc nodeWeights
        val (nodeWeights, edgeWeights) = if (null != model) {
          model.getCommNodes(keys).get(keys) -> model.getCommEdges(keys).get(keys)
        } else {
          Array.fill(keys.length)(1.0F) -> Array.fill(keys.length)(0.0F)
        }
        CSR(i, keys, nodeWeights, edgeWeights, neighbors, weights, batchSize)
      }
    updateGraphMinMax(tmp)
  }

  private def getMinMaxMap(id2length: Array[(Int, Int)]): Map[Long, (Long, Long)] = {
    var index = 0
    val map = mutable.HashMap[Long, (Long, Long)]()
    id2length.foreach { case (id, length) =>
      map.put(id, (index, index + length - 1))
      index = index + length
    }
    map.toMap
  }

  // community min and max id in partition graph
  def updateGraphMinMax(graphs: RDD[CSR]): RDD[CSR] = {
    val sc = graphs.sparkContext
    val id2size = graphs.map(x => x.id -> x.superNodes.length).collect().sortBy(_._1)
    val map = sc.broadcast(getMinMaxMap(id2size))
    graphs.map { g =>
      val (min, max) = map.value(g.id)
      g.setMin(min)
      g.setMax(max)
      g
    }
  }
}
