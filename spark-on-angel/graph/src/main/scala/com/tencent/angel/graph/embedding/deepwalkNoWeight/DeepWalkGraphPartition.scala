package com.tencent.angel.graph.embedding.deepwalkNoWeight

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

class DeepWalkGraphPartition(index: Int, srcNodesArray: Array[Long], srcNodesSamplePaths: Array[Array[Long]], batchSize: Int) {

  def process(model: DeepWalkPSModel, iteration: Int, dynamicInitNeighbor: Boolean=false): DeepWalkGraphPartition = {
    println(s"partition $index: ---------- iteration $iteration starts ----------")
    val rnd = new Random()
    //sample nodes path batch by batch
    srcNodesArray.indices.sliding(batchSize, batchSize).foreach { nodesIndex =>
      //the tail nodes set of paths
      val pullNodes = srcNodesSamplePaths.slice(nodesIndex.head, nodesIndex.last + 1)
        .map(a => (a.last, 1)).groupBy(_._1).map(t => (t._1, t._2.size))
      val (nodes, count) = pullNodes.unzip

      //pull nodes neighbors
      val beforeSample = System.currentTimeMillis()
      val nodesToNeighboes = model.getSampledNeighbors(model.edgesPsMatrix, nodes.toArray, count.toArray, dynamicInitNeighbor)
      //      println(s"partition $index, iter $iteration, sampleTime: ${System.currentTimeMillis() - beforeSample} ms")

      //process each node
      for (idx <- nodesIndex) {
        val oldPath = srcNodesSamplePaths(idx) // the old path of idx node
        val oldPathTail = oldPath.last // the tail node of path
        val tailNeighbors = nodesToNeighboes.get(oldPathTail) //the neighbors of tail node
        if (tailNeighbors.nonEmpty) {
          val sampleFromNeighbors = tailNeighbors(rnd.nextInt(tailNeighbors.size)) // sample a node randomly from tail node's neighbors
          srcNodesSamplePaths(idx) = Array.concat(oldPath, Array(sampleFromNeighbors)) // merge old path and sample node
        }

      }
    }

    println(s"partition $index: ---------- iteration $iteration terminated ----------")
    new DeepWalkGraphPartition(index, srcNodesArray, srcNodesSamplePaths, batchSize)

  }

  def save(): Array[Array[Long]] =
    srcNodesSamplePaths

  def deepClone(): DeepWalkGraphPartition = {
    val newSrcNodesArray = srcNodesArray.clone()
    val newPaths = new Array[Array[Long]](srcNodesSamplePaths.length)
    var i = 0
    while (i < srcNodesSamplePaths.length) {
      newPaths(i) = srcNodesSamplePaths(i)
      i += 1
    }
    new DeepWalkGraphPartition(index, newSrcNodesArray, newPaths, batchSize)
  }
}


object DeepWalkGraphPartition {
  def initPSMatrix(model: DeepWalkPSModel, index: Int, iterator: Iterator[(Long, Array[Long])], batchSize: Int): Iterator[Long] = {
    iterator.sliding(batchSize, batchSize).map { pairs =>
      val nodeId2Neighbors = new Long2ObjectOpenHashMap[NeighborsTableElement](pairs.length)
      pairs.foreach { case (src, neighbors) =>
        val elem = new NeighborsTableElement(neighbors)
        nodeId2Neighbors.put(src, elem)
      }
      model.initNodeNeiWithNoWeight(nodeId2Neighbors)
      nodeId2Neighbors.clear()
      pairs.length.toLong
    }
  }

  def initNodePaths(index: Int, iterator: Array[Long], batchSize: Int): DeepWalkGraphPartition = {
    val srcNodesSamplePaths = ArrayBuffer[Array[Long]]()
    iterator.foreach { node =>
      srcNodesSamplePaths.append(Array(node))
    }
    new DeepWalkGraphPartition(index, iterator, srcNodesSamplePaths.toArray, batchSize)
  }
}