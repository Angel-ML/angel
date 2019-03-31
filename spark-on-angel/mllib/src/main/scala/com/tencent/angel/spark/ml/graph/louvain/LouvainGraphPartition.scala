package com.tencent.angel.spark.ml.graph.louvain

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.util.collection.SparkCollectionProxy

import com.tencent.angel.ml.math2.storage.IntIntSparseVectorStorage
import com.tencent.angel.ml.math2.vector.{IntFloatVector, IntIntVector}


class LouvainGraphPartition(
    var keys: Array[Int],
    var adjs: Array[Array[Int]],
    var weights: Array[Array[Float]]) extends Serializable {
  assert(keys.length == adjs.length)
  private lazy val allNodes = (adjs.flatten ++ keys).distinct

  private var nodeWeights: Array[Float] = _

  def setNodeWeights(nodeWeights: Array[Float]): this.type = {
    this.nodeWeights = nodeWeights
    this
  }

  def calcNodeWeightsFromEdgeWeights: Array[Float] = {
    this.nodeWeights = weights.map(_.sum)
    this.nodeWeights
  }

  private[louvain] def max: Int = keys.aggregate(Int.MinValue)(math.max, math.max)

  private[louvain] def partitionWeights: Float = nodeWeights.sum

  def createNewEdges(model: LouvainPSModel, batchSize: Int): Iterator[((Int, Int), Float)] = {
    makeBatches(batchSize).flatMap { batch =>
      val (start, end) = batch
      val id2comm = model.fetchNode2Community(start, end, keys, adjs)
      Iterator.range(start, end).flatMap { i =>
        val u = id2comm.get(keys(i))
        val adj = adjs(i)
        val weight = weights(i)
        adj.indices.flatMap { j =>
          val v = id2comm.get(adj(j))
          if (u < v) {
            Iterator.single(((u, v), weight(j)))
          } else if (v < u) {
            Iterator.single(((v, u), weight(j)))
          } else {
            Iterator.empty
          }
        }
      }
    }
  }

  def communityIds(model: LouvainPSModel): Array[Int] = {
    model.getCommunities(keys).distinct
  }

  def community2nodes(model: LouvainPSModel): Array[(Int, Int)] = {
    val pairs = model.getCommunities(keys).zip(keys)
    pairs
  }

  def modularityOptimize(
                          model: LouvainPSModel,
                          totalWeight: Double,
                          batchSize: Int = 100,
                          shuffle: Boolean = true): Unit = {

    if (shuffle) {
      this.shuffle()
    }

    for (batch <- makeBatches(batchSize)) {
      val (start, end) = batch
      val (id2comm, comm2info) = model.fetch(start, end, keys, adjs)
      val updatedNodeBuffer = new ArrayBuffer[Int]()
      val updatedCommBuffer = new ArrayBuffer[Int]()
      val communityWeightDelta = SparkCollectionProxy.createOpenHashMap[Int, Float]()

      for (i <- start until end) {
        val node = keys(i)
        val curComm = id2comm.get(node)
        val best = bestCommunityInNeighbors(i, id2comm, comm2info, totalWeight)
        if (best != curComm) {
          updatedNodeBuffer += node
          updatedCommBuffer += best
          val delta = nodeWeights(i)
          communityWeightDelta.changeValue(curComm, -delta, _ - delta)
          communityWeightDelta.changeValue(best, delta, _ + delta)
        }
      }

      model.updateNode2community(updatedNodeBuffer.toArray, updatedCommBuffer.toArray)
      val (comm, delta) = communityWeightDelta.unzip
      model.incrementCommWeight(comm.toArray, delta.toArray)
    }
  }

  private def makeBatches(batchSize: Int): Iterator[(Int, Int)] = new Iterator[(Int, Int)] {
    var index = 0

    override def next(): (Int, Int) = {
      val preIndex = index
      index = index + batchSize
      (preIndex, math.min(index, keys.length))
    }

    override def hasNext: Boolean = {
      index < keys.length
    }
  }

  // move node to a single node community, with delta = (k_i * \sigma_tol - k_i^2 / 2m - k_{i,in}) / m
  // move node to target community, with delta = (k_{i,in} - k_i * \sigma_tol / 2m) / m
  private def bestCommunityInNeighbors(
                                        i: Int,
                                        id2comm: IntIntVector,
                                        comm2info: IntFloatVector,
                                        total: Double): Int = {

    val comm2edgeWeight = SparkCollectionProxy.createOpenHashMap[Int, Float]()
    for (j <- adjs(i).indices) {
      comm2edgeWeight.changeValue(id2comm.get(adjs(i)(j)), weights(i)(j), _ + weights(i)(j))
    }
    val curComm = id2comm.get(keys(i))
    var best = curComm
    var threshold = comm2edgeWeight.changeValue(curComm, 0, x => x) -
      nodeWeights(i) * (comm2info.get(curComm) - nodeWeights(i)) / total
    comm2edgeWeight.foreach { case (comm, eWeight) =>
      if (comm != curComm) {
        val delta = eWeight - nodeWeights(i) * comm2info.get(comm) / total
        if (delta > threshold) {
          best = comm
          threshold = delta
        }
      }
    }
    best
  }

  private def shuffle(): Unit = {
    val newIndices = Random.shuffle(keys.indices.toList).toArray
    keys = newIndices.map(keys.apply)
    adjs = newIndices.map(adjs.apply)
    weights = newIndices.map(weights.apply)
    nodeWeights = newIndices.map(nodeWeights.apply)
  }

  private[louvain] def sumOfWeightsBetweenCommunity(model: LouvainPSModel): Double = {
    val local2comm = model.node2CommunityPSVector.pull(allNodes).asInstanceOf[IntIntVector]
    var weight = 0.0
    for (i <- keys.indices) {
      val comm = local2comm.get(keys(i))
      for (j <- adjs(i).indices) {
        if (comm != local2comm.get(adjs(i)(j))) {
          weight += weights(i)(j)
        }
      }
    }
    weight
  }

  private[louvain] def node2community(model: LouvainPSModel): Array[(Int, Int)] = {
    val keyCopy = new Array[Int](keys.length)
    System.arraycopy(keys, 0, keyCopy, 0, keys.length)
    model.node2CommunityPSVector.pull(keyCopy).asInstanceOf[IntIntVector]
      .getStorage.asInstanceOf[IntIntSparseVectorStorage]
      .entryIterator().map { entry =>
      (entry.getIntKey, entry.getIntValue)
    }.toArray
  }

  private[louvain] def initializePS(model: LouvainPSModel, preserve: Boolean = false): Unit = {
    if (preserve) {
      this.nodeWeights = model.getCommInfo(keys).get(keys)
    } else {
      this.nodeWeights = weights.map(_.sum)
      model.initialize(keys, this.nodeWeights)
    }
  }
}

