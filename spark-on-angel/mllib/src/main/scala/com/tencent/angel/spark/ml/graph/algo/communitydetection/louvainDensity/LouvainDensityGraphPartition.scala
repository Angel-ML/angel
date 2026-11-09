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
package com.tencent.angel.spark.ml.graph.algo.communitydetection.louvainDensity

import com.tencent.angel.ml.math2.vector.{LongFloatVector, LongLongVector}
import org.apache.spark.SparkPrivateClassProxy

import scala.collection.mutable.ArrayBuffer
import scala.util.Random


class LouvainDensityGraphPartition(
                                    var superNodes: Array[Long],
                                    var adj: Array[Array[Long]],
                                    var adjWeights: Array[Array[Float]],
                                    var adjEdgesNumber: Array[Array[Float]],
                                    var partRelatedNodeWeights: Array[Float],
                                    var partRelatedNodeNumberSelfEdge: Array[Float],
                                    var partRelatedNodeSize: Array[Float],
                                    var partRelatedNodeSelfWeight: Array[Float]) extends Serializable {

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
  def partFolding(model: LouvainDensityPSModel, batchSize: Int): Iterator[((Long, Long), (Float, Float))] = {
    this.makeBatchIterator(batchSize).flatMap { batch =>
      val (start, end) = batch
      val nodes = (this.adj.slice(start, end).flatten ++ this.superNodes.slice(start, end)).distinct
      val node2comm = model.getNode2commMap(nodes)

      /*
      println("+++++++++++partFolding++++++++")
      println("in infor")
      model.getNode2commPairsArr(nodes).sortBy(_._2).foreach(println)
      println("out infor")
      model.node2CommunityOutPSVector.pull(nodes.clone()).asInstanceOf[LongLongVector].get(nodes).zip(nodes).sortBy(_._1).map(a => (a._2, a._1)).foreach(println)
       */

      Iterator.range(start, end).flatMap { i =>
        val u = node2comm.get(this.superNodes(i))
        val neighbors = this.adj(i)
        val edgeWeights = this.adjWeights(i)
        val edgeNumber = this.adjEdgesNumber(i)

        neighbors.indices.flatMap { j =>
          val v = node2comm.get(neighbors(j))
          if (u < v) {
            Iterator.single(((u, v), (edgeWeights(j), edgeNumber(j))))
          } else if (v < u) {
            Iterator.single(((v, u), (edgeWeights(j), edgeNumber(j))))
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
  def partCommunityIds(model: LouvainDensityPSModel): Array[Long] = {
    //println("-----superNodes-----")
    //println(superNodes.mkString(","))
    model.getCommunitiesFromInVector(superNodes).distinct
  }

  /**
   * get the distinct communities in the partition
   *
   * @param model louvain ps model
   * @return
   */
  def partCommunityIdsFromBoth(model: LouvainDensityPSModel, source: String): Array[Long] = {
    if (source == "in") {
      model.getCommunitiesFromInVector(superNodes).distinct
    }
    else {
      model.getCommunitiesFromOutVector(superNodes).distinct
    }

  }


  /**
   * get the pair(community,node) in the partition
   *
   * @param model louvain ps model
   * @return
   */
  def partComm2nodeParis(model: LouvainDensityPSModel): Array[(Long, Long)] = {
    val pairs = model.getCommunitiesFromInVector(superNodes).zip(superNodes)
    pairs
  }

  /**
   * get the pair(community,node) in the partition
   *
   * @param model louvain ps model
   * @return
   */
  def partComm2nodeParisFromOutVector(model: LouvainDensityPSModel): Array[(Long, Long)] = {
    val pairs = model.getCommunitiesFromOutVector(superNodes).zip(superNodes)
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
                          model: LouvainDensityPSModel,
                          totalWeight: Double,
                          batchSize: Int,
                          shuffle: Boolean = true,
                          preserveRate: Double,
                          globalDensity: Double): Unit = {

    if (shuffle) {
      this.shuffle()
    }

    //println("in in partition modularityOptimize ")
    for (batch <- makeBatchIterator(batchSize)) {
      val (start, end) = batch
      val nodes = (this.adj.slice(start, end).flatten ++ this.superNodes.slice(start, end)).distinct
      val (id2comm, comm2Weight, comm2NumberSelfEdge, comm2Size, comm2SelfWeight) = model.getModelPart(nodes)
      val updatedNodeBuffer = new ArrayBuffer[Long]()
      val updatedCommBuffer = new ArrayBuffer[Long]()
      //val communityInforDelta = SparkPrivateClassProxy.createOpenHashMap[Long, (Float, Float, Float, Float)]()

      for (i <- start until end) {
        val node = superNodes(i)
        val curComm = id2comm.get(node)

        val best = if (Random.nextDouble() > preserveRate) {
          bestCommunityInNeighbors(i, id2comm, comm2Weight, totalWeight, comm2NumberSelfEdge, comm2Size, comm2SelfWeight, globalDensity)
        }
        else {
          curComm
        }
        updatedNodeBuffer += node
        updatedCommBuffer += best
      }
      //println("--update node comm-------")
      //println(updatedNodeBuffer.toArray.mkString(" "))
      //println(updatedCommBuffer.toArray.mkString(" "))
      model.updateNode2communityOut(updatedNodeBuffer.toArray, updatedCommBuffer.toArray)
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
   * @param i           node index
   * @param id2comm     map(node id -> community id )
   * @param comm2Weight map(community id -> community weight )
   * @param total       2m for unweighted graph ; 2*sum(edge weights) for weighted graph
   * @return the best community id of node i
   */
  private def bestCommunityInNeighbors(
                                        i: Int,
                                        id2comm: LongLongVector,
                                        comm2Weight: LongFloatVector,
                                        total: Double,
                                        comm2NumberSelfEdge: LongFloatVector,
                                        comm2Size: LongFloatVector,
                                        comm2SelfWeight: LongFloatVector,
                                        globalDensity: Double
                                      ): Long = {

    val comm2edgeWeight = SparkPrivateClassProxy.createOpenHashMap[Long, (Float, Float)]()

    val curComm = id2comm.get(this.superNodes(i))
    comm2edgeWeight.changeValue(curComm, (0f, 0f), { a => (0f, 0f) })

    for (j <- this.adj(i).indices) {
      comm2edgeWeight.changeValue(id2comm.get(this.adj(i)(j)), (this.adjWeights(i)(j), this.adjEdgesNumber(i)(j)), { a => (a._1 + this.adjWeights(i)(j), a._2 + this.adjEdgesNumber(i)(j)) })
    }

    val size = comm2Size.get(curComm)
    val numberSelfEdge = comm2NumberSelfEdge.get(curComm)
    val selfWeight = comm2SelfWeight.get(curComm)
    val weightDegree = comm2Weight.get(curComm)
    val p = if (size <= 1) 0 else 2 * numberSelfEdge / (size * (size - 1)) - globalDensity
    val oriValueCurComm = 2 * selfWeight * p - Math.pow(weightDegree * p, 2) / total //???? *2

    val (eWeightCurComm, eNumberCurComm) = comm2edgeWeight(curComm)
    val removeSize = size - partRelatedNodeSize(i)
    val removeNumberSelfEdge = numberSelfEdge - eNumberCurComm - partRelatedNodeNumberSelfEdge(i)
    val removeSelfWeight = selfWeight - eWeightCurComm - partRelatedNodeSelfWeight(i)
    val removeWeightDegree = weightDegree - partRelatedNodeWeights(i)
    val removeP = if (removeSize <= 1) 0 else 2 * removeNumberSelfEdge / (removeSize * (removeSize - 1)) - globalDensity
    val removeValueCurComm = 2 * removeSelfWeight * removeP - Math.pow(removeWeightDegree * removeP, 2) / total //???? *2

    var bestValue = oriValueCurComm - removeValueCurComm
    var best = curComm


    comm2edgeWeight.foreach { case (comm, (eWeight, eNumber)) =>
      if (comm != curComm) {
        val sizeAddTmp = comm2Size.get(comm) + partRelatedNodeSize(i)
        val numberSelfEdgeAddTmp = comm2NumberSelfEdge.get(comm) + partRelatedNodeNumberSelfEdge(i) + eNumber
        val selfWeightAddTmp = comm2SelfWeight.get(comm) + eWeight + partRelatedNodeSelfWeight(i)
        val weightDegreeAddTmp = comm2Weight.get(comm) + partRelatedNodeWeights(i)
        val pAddTmp = if (sizeAddTmp <= 1) 0  else 2 * numberSelfEdgeAddTmp / (sizeAddTmp * (sizeAddTmp - 1)) - globalDensity
        val qAddTmp = 2 * selfWeightAddTmp * pAddTmp - Math.pow(weightDegreeAddTmp * pAddTmp, 2) / total //???? *2

        val sizeOriTmp = comm2Size.get(comm)
        val numberSelfEdgeOriTmp = comm2NumberSelfEdge.get(comm)
        val selfWeightOriTmp = comm2SelfWeight.get(comm)
        val weightDegreeOriTmp = comm2Weight.get(comm)
        val pOriTmp = if (sizeOriTmp <= 1) 0 else 2 * numberSelfEdgeOriTmp / (sizeOriTmp * (sizeOriTmp - 1)) - globalDensity
        val qOriTmp = 2 * selfWeightOriTmp * pOriTmp - Math.pow(weightDegreeOriTmp * pOriTmp, 2) / total //???? *2

        val bestValueTmp = qAddTmp - qOriTmp
        if (bestValueTmp -bestValue> 0.0) {
          best = comm
          bestValue = bestValueTmp
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

  private[louvainDensity] def partitionWeights: Float = {
    partRelatedNodeWeights.sum
  }

  /*
    private[louvainDensity] def getEdgesInforsFromOutVector(model: LouvainDensityPSModel, batchSize: Int): Iterator[(Long, (Float, Float))] = {
      this.makeBatchIterator(batchSize).flatMap { batch =>
        val (start, end) = batch
        val nodes = (this.adj.slice(start, end).flatten ++ this.superNodes.slice(start, end)).distinct
        val node2comm = model.getNode2commMapFromOutVector(nodes)

        //println("------getEdgesInforsInCommunity-----")
        //println(nodes.mkString(" "))
        Iterator.range(start, end).flatMap { i =>
          val u = node2comm.get(this.superNodes(i))
          val neighbors = this.adj(i)
          val edgeWeights = this.adjWeights(i)

          neighbors.indices.flatMap { j =>
            val v = node2comm.get(neighbors(j))
            if (u == v) {
              Iterator.single((u, (edgeWeights(j), 1.0f)))
            }
            else {
              Iterator.empty
            }
          }
        }
      }
    }

   */


  private[louvainDensity] def getEdgesInforsFromBothVector(model: LouvainDensityPSModel, batchSize: Int, source: String): Iterator[(Long, (Float, Float))] = {
    this.makeBatchIterator(batchSize).flatMap { batch =>
      val (start, end) = batch
      val nodes = (this.adj.slice(start, end).flatten ++ this.superNodes.slice(start, end)).distinct
      val node2comm = model.getNode2commMapFromBothVector(nodes, source)

      //println("------getEdgesInforsInCommunity-----")
      //println(nodes.mkString(" "))
      Iterator.range(start, end).flatMap { i =>
        val u = node2comm.get(this.superNodes(i))
        val neighbors = this.adj(i)
        val edgeWeights = this.adjWeights(i)
        val edgeNumber = this.adjEdgesNumber(i)

        var newSelfWeightTmp = partRelatedNodeSelfWeight(i)
        var newNumberSelfEdgeTmp = partRelatedNodeNumberSelfEdge(i)

        neighbors.indices.foreach { j =>
          val v = node2comm.get(neighbors(j))
          if (u == v) {
            newSelfWeightTmp += edgeWeights(j) / 2.0f
            newNumberSelfEdgeTmp += edgeNumber(j) / 2.0f
          }
        }

        Iterator.single((u, (newSelfWeightTmp, newNumberSelfEdgeTmp)))

        /*

        if (neighborCommunity.contains(u)) {
          neighbors.indices.flatMap { j =>
            val v = node2comm.get(neighbors(j))
            if (u == v) {
              val newNumberSelfEdge = partRelatedNodeNumberSelfEdge(i) + edgeNumber(j) / 2.0f
              val newSelfWeight = partRelatedNodeSelfWeight(i) + edgeWeights(j) / 2.0f
              //println("==========")
              //println("------getEdgesInforsFromBothVector-----")
              //println(superNodes(i), neighbors(j), u, u, newSelfWeight, newNumberSelfEdge)
              Iterator.single((u, (newSelfWeight, newNumberSelfEdge)))
            }
            else {
              //println("==========")
              //println("------getEdgesInforsFromBothVector   not in the same-----")
              //println(u,partRelatedNodeSelfWeight(i),partRelatedNodeNumberSelfEdge(i))

              //val newNumberSelfEdge = partRelatedNodeNumberSelfEdge(i)
              //val newSelfWeight = partRelatedNodeSelfWeight(i)
              //println("==========")
              //println("------getEdgesInforsFromBothVector-----")
              //println(superNodes(i), neighbors(j), u, v, newSelfWeight, newNumberSelfEdge)
              //Iterator.single((u, (newSelfWeight, newNumberSelfEdge)))
              //Iterator.single((u, (0.0f,0.0f)))
              Iterator.empty
            }

          }
        }
        else {
          val newNumberSelfEdge = partRelatedNodeNumberSelfEdge(i)
          val newSelfWeight = partRelatedNodeSelfWeight(i)
          //println("==========")
          Iterator.single((u, (newSelfWeight, newNumberSelfEdge)))
        }

         */


      }
    }
  }

  /*
  private[louvainDensity] def getNodesInforsFromOutVector(model: LouvainDensityPSModel, batchSize: Int): Iterator[(Long, (Float, Float))] = {
    this.makeBatchIterator(batchSize).flatMap { batch =>
      val (start, end) = batch
      val nodes = this.adj.slice(start, end).flatten
      val node2comm = model.getNode2commMapFromOutVector(nodes)

      Iterator.range(start, end).map { i =>
        val comm = node2comm.get(this.superNodes(i))
        val weight = partRelatedNodeWeights(i)
        (comm, (weight, 1f))
      }
    }
  }

   */

  private[louvainDensity] def getNodesInforsFromBothVector(model: LouvainDensityPSModel, batchSize: Int, source: String): Iterator[(Long, (Float, Float))] = {
    this.makeBatchIterator(batchSize).flatMap { batch =>
      val (start, end) = batch
      val nodes = this.superNodes.slice(start, end)
      val node2comm = model.getNode2commMapFromBothVector(nodes, source: String)

      Iterator.range(start, end).map { i =>
        val comm = node2comm.get(this.superNodes(i))
        val weight = partRelatedNodeWeights(i)
        //val newNodeSize = if (1.0 > partRelatedNodeSize(i)) 1.0f else partRelatedNodeSize(i)
        val newNodeSize =  partRelatedNodeSize(i)
        (comm, (weight, newNodeSize))
      }
    }
  }


  private[louvainDensity] def node2community(model: LouvainDensityPSModel): Array[(Long, Long)] = {
    model.getNode2commPairsArr(this.superNodes.clone())
  }

  private[louvainDensity] def updateNodeWeightsToPS(model: LouvainDensityPSModel): Unit = {
    model.setNode2commAndComm2weight(superNodes, this.partRelatedNodeWeights)
    //println(s"first 5 is ${this.partRelatedNodeIds.take(5).mkString(",")}")
  }

}

