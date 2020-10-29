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

import com.tencent.angel.ml.math2.vector.{LongFloatVector, LongLongVector}
import org.apache.spark.SparkPrivateClassProxy

import scala.collection.mutable.ArrayBuffer
import scala.util.Random


class LouvainGraphPartition(
                             var superNodes: Array[Long],
                             var adj: Array[Array[Long]],
                             var adjWeights: Array[Array[Float]],
                             var partRelatedNodeWeights: Array[Float]) extends Serializable {

  assert(superNodes.length == adj.length)
  assert(adj.length == adjWeights.length)

  private lazy val partRelatedNodeIds = (adj.flatten ++ superNodes).distinct

  /**
   * get the list(community of node,community of neighbor j)
   *
   * @param model     louvain ps model
   * @param batchSize the compute size
   * @return new edges(conver node-neighbor to community of node-community of neighbor j)
   */
  def partFolding(model: LouvainPSModel, batchSize: Int): Iterator[((Long, Long), Float)] = {
    this.makeBatchIterator(batchSize).flatMap { batch =>
      val (start, end) = batch
      val nodes = (this.adj.slice(start, end).flatten ++ this.superNodes.slice(start, end)).distinct
      val node2comm = model.getNode2commMap(nodes)

      Iterator.range(start, end).flatMap { i =>
        val u = node2comm.get(this.superNodes(i))
        val neighbors = this.adj(i)
        val edgeWeights = this.adjWeights(i)

        neighbors.indices.flatMap { j =>
          val v = node2comm.get(neighbors(j))
          if (u < v) {
            Iterator.single(((u, v), edgeWeights(j)))
          } else if (v < u) {
            Iterator.single(((v, u), edgeWeights(j)))
          } else {
            Iterator.empty
          }
        }
      }
    }
  }

  /**
   * get the distinct communities in the partition
   *
   * @param model louvain ps model
   * @return
   */
  def partCommunityIds(model: LouvainPSModel): Array[Long] = {
    model.getCommunities(superNodes).distinct
  }

  /**
   * get the pair(community,node) in the partition
   *
   * @param model louvain ps model
   * @return
   */
  def partComm2nodeParis(model: LouvainPSModel): Array[(Long, Long)] = {
    val pairs = model.getCommunities(superNodes).zip(superNodes)
    pairs
  }


  /**
   * move each node to neighbors' community in order to get the max modularity gain
   *
   * @param model       the ps model
   * @param totalWeight 2m for unweighted graph ; 2*sum(edge weights) for weighted graph
   * @param batchSize   the compute unit
   * @param shuffle     whether to shuffle data
   */
  def modularityOptimize(
                          model: LouvainPSModel,
                          totalWeight: Double,
                          batchSize: Int,
                          shuffle: Boolean = true,
                          preserveRate: Double): Unit = {

    if (shuffle) {
      this.shuffle()
    }

    for (batch <- makeBatchIterator(batchSize)) {
      val (start, end) = batch
      val nodes = (this.adj.slice(start, end).flatten ++ this.superNodes.slice(start, end)).distinct
      val (id2comm, comm2info) = model.getModelPart(nodes)
      val updatedNodeBuffer = new ArrayBuffer[Long]()
      val updatedCommBuffer = new ArrayBuffer[Long]()
      val communityWeightDelta = SparkPrivateClassProxy.createOpenHashMap[Long, Float]()

      for (i <- start until end) {
        val node = superNodes(i)
        val curComm = id2comm.get(node)
        //val best = bestCommunityInNeighbors(i, id2comm, comm2info, totalWeight)

        val best = if (Random.nextDouble() > preserveRate) {
          bestCommunityInNeighbors(i, id2comm, comm2info, totalWeight)
        }
        else {
          curComm
        }

        if (best != curComm) {
          updatedNodeBuffer += node
          updatedCommBuffer += best
          val delta = partRelatedNodeWeights(i)
          communityWeightDelta.changeValue(curComm, -delta, _ - delta)
          communityWeightDelta.changeValue(best, delta, _ + delta)
        }
      }

      model.updateNode2community(updatedNodeBuffer.toArray, updatedCommBuffer.toArray)
      val (comm, delta) = communityWeightDelta.unzip
      model.incrementCommWeight(comm.toArray, delta.toArray)
    }
  }

  /**
   * make the batch iterator list
   *
   * @param batchSize
   * @return
   */
  private def makeBatchIterator(batchSize: Int): Iterator[(Int, Int)] = new Iterator[(Int, Int)] {
    var index = 0

    override def next(): (Int, Int) = {
      val preIndex = index
      index = index + batchSize
      (preIndex, math.min(index, superNodes.length))
    }

    override def hasNext: Boolean = {
      index < superNodes.length
    }
  }

  // move node to a single node community, with delta = (k_i * \sigma_tol - k_i^2 / 2m - k_{i,in}) / m
  // move node to target community, with delta = (k_{i,in} - k_i * \sigma_tol / 2m) / m
  /**
   *
   * @param i         node index
   * @param id2comm   map(node id -> community id )
   * @param comm2info map(community id -> community weight )
   * @param total     2m for unweighted graph ; 2*sum(edge weights) for weighted graph
   * @return the best community id of node i
   */
  private def bestCommunityInNeighbors(
                                        i: Int,
                                        id2comm: LongLongVector,
                                        comm2info: LongFloatVector,
                                        total: Double): Long = {

    val comm2edgeWeight = SparkPrivateClassProxy.createOpenHashMap[Long, Float]()
    for (j <- this.adj(i).indices) {
      comm2edgeWeight.changeValue(id2comm.get(this.adj(i)(j)), this.adjWeights(i)(j), _ + this.adjWeights(i)(j))
    }
    val curComm = id2comm.get(this.superNodes(i))
    var best = curComm
    var threshold = comm2edgeWeight.changeValue(curComm, 0, x => x) -
      partRelatedNodeWeights(i) * (comm2info.get(curComm) - partRelatedNodeWeights(i)) / total
    comm2edgeWeight.foreach { case (comm, eWeight) =>
      if (comm != curComm) {
        val delta = eWeight - partRelatedNodeWeights(i) * comm2info.get(comm) / total
        if (delta > threshold) {
          best = comm
          threshold = delta
        }
      }
    }
    best
  }

  /**
   * shuffle the data in partition randomly
   */
  private def shuffle(): Unit = {
    val newIndices = Random.shuffle(this.superNodes.indices.toList).toArray
    this.superNodes = newIndices.map(this.superNodes.apply)
    this.adj = newIndices.map(this.adj.apply)
    this.adjWeights = newIndices.map(this.adjWeights.apply)
    partRelatedNodeWeights = newIndices.map(partRelatedNodeWeights.apply)
  }

  private[louvain] def partitionWeights: Float = {
    partRelatedNodeWeights.sum
  }

  private[louvain] def sumOfWeightsBetweenCommunity(model: LouvainPSModel): Double = {
    val local2comm = model.node2CommunityPSVector.pull(partRelatedNodeIds).asInstanceOf[LongLongVector]
    var weight = 0.0
    for (i <- superNodes.indices) {
      val comm = local2comm.get(superNodes(i))
      for (j <- adj(i).indices) {
        if (comm != local2comm.get(adj(i)(j))) {
          weight += adjWeights(i)(j)
        }
      }
    }
    weight
  }

  private[louvain] def node2community(model: LouvainPSModel): Array[(Long, Long)] = {
    model.getNode2commPairsArr(this.superNodes.clone())
  }

  private[louvain] def updateNodeWeightsToPS(model: LouvainPSModel): Unit = {
    model.setNode2commAndComm2weight(superNodes, this.partRelatedNodeWeights)
    //println(s"first 5 is ${this.partRelatedNodeIds.take(5).mkString(",")}")
  }

}

