package com.tencent.angel.graph.embedding.struct2vec

import com.tencent.angel.graph.psf.neighbors.SampleNeighborsWithCount.NeighborsAliasTableElement

import scala.util.Random


import scala.collection.mutable.ArrayBuffer
import scala.util.Random


class Struct2vecGraphPartition(index: Int, srcNodesArray: Array[Long], srcNodesSamplePaths: Array[Array[Long]], batchSize: Int) {

  def process(model: Struct2vecPSModel, iteration: Int): Struct2vecGraphPartition = {
    println(s"partition $index: ---------- iteration $iteration starts ----------")
    val rnd = new Random()
    //按块读取
    srcNodesArray.indices.sliding(batchSize, batchSize).foreach { nodesIndex =>
      //尾节点集合
      val pullNodes = srcNodesSamplePaths.slice(nodesIndex.head, nodesIndex.last + 1)
        .map(a => (a.last, 1)).groupBy(_._1).map(t => (t._1, t._2.size))
      val (nodes, count) = pullNodes.unzip

      //获取节点的邻居
      val beforeSample = System.currentTimeMillis()
      val nodesToNeighboes = model.getSampledNeighbors(model.edgesPsMatrix, nodes.toArray, count.toArray)

      println(s"partition $index, iter $iteration, sampleTime: ${System.currentTimeMillis() - beforeSample} ms")

      //遍历每一个节点  nodesIndex 节点列表
      for (idx <- nodesIndex) {
        val oldPath = srcNodesSamplePaths(idx) // 前一个节点的下标
        val oldPathTail = oldPath.last // 尾节点路径
        val tailNeighbors = nodesToNeighboes.get(oldPathTail) //尾节点的邻居
        if (tailNeighbors.nonEmpty) {
          val sampleFromNeighbors = tailNeighbors(rnd.nextInt(tailNeighbors.size)) // sample a node randomly from tail node's neighbors
          srcNodesSamplePaths(idx) = Array.concat(oldPath, Array(sampleFromNeighbors)) // merge old path and sample node
        }

      }
    }

    println(s"partition $index: ---------- iteration $iteration terminated ----------")

    //return idx2Node -> index  srcNodesArray -> node2idx
    new Struct2vecGraphPartition(index, srcNodesArray, srcNodesSamplePaths, batchSize)

  }


  //存储路径
  def save(): Array[Array[Long]] =
    srcNodesSamplePaths

  def deepClone(): Struct2vecGraphPartition = {
    val newSrcNodesArray = srcNodesArray.clone()
    val newPaths = new Array[Array[Long]](srcNodesSamplePaths.length)
    var i = 0
    while (i < srcNodesSamplePaths.length) {
      newPaths(i) = srcNodesSamplePaths(i)
      i += 1
    }
    new Struct2vecGraphPartition(index, newSrcNodesArray, newPaths, batchSize)
  }
}


//单例对象
object Struct2vecGraphPartition {
  def initPSMatrixAndNodePath(model: Struct2vecPSModel, index: Int, iterator: Iterator[(Long, Array[Long], Array[Float], Array[Int])], batchSize: Int): DeepWalkGraphPartition = {
    val srcNodes = new LongArrayList()
    iterator.sliding(batchSize, batchSize).foreach { pairs =>
      val nodeId2Neighbors = new Long2ObjectOpenHashMap[NeighborsAliasTableElement](pairs.length)

      pairs.foreach { case (src, neighbors, accept, alias) =>
        val elem = new NeighborsAliasTableElement(neighbors, accept, alias)
        nodeId2Neighbors.put(src, elem)
        srcNodes.add(src)
      }
      model.initNodeNei(nodeId2Neighbors)
      nodeId2Neighbors.clear()
    }
    initNodePaths(index,srcNodes.toLongArray(),batchSize)
  }

  def initNodePaths(index: Int, iterator: Array[Long], batchSize: Int): Struct2vecGraphPartition = {
    val srcNodesSamplePaths = ArrayBuffer[Array[Long]]()
    iterator.foreach { node =>
      srcNodesSamplePaths.append(Array(node))
    }
    new Struct2vecGraphPartition(index, iterator, srcNodesSamplePaths.toArray, batchSize)
  }
}

