package com.tencent.angel.spark.ml.graph.algo.embedding.node2vec

import com.tencent.angel.spark.models.PSMatrix
import com.tencent.angel.graph.client.node2vec.getfuncs.pullneighbor._
import com.tencent.angel.graph.common.psf.param.LongKeysGetParam
import com.tencent.angel.graph.common.psf.result.GetLongsResult
import com.tencent.angel.graph.model.neighbor.samplewithtrunc.SampleTruncParam
import com.tencent.angel.spark.ml.graph.algo.embedding.node2vec.Node2VecGraphPartition.getByteNeighborWithTrunc
import com.tencent.angel.spark.ml.psf.node2vec.GetNeighborWithTrunc
import it.unimi.dsi.fastutil.longs.{Long2DoubleOpenHashMap, Long2LongOpenHashMap, Long2ObjectOpenHashMap, LongArrayList, LongOpenHashSet}
import com.tencent.angel.spark.ml.psf.node2vec.GetAliasTableWithTrunc

import scala.util.Random
import scala.collection.mutable.ArrayBuffer

class Node2VecGraphPartition(index: Int, srcNodesArray: Array[Long],
                             srcNodesSamplePaths: Array[Array[Long]],
                             batchSize: Int, p: Double, q:Double,
                             currentIter: Int, useTrunc: Boolean,
                             truncLength: Int) {

  private val upperBound: Double = Math.max(1.0, Math.max(1 / p, 1 / q))
  private val lowerBound: Double = Math.min(1.0, Math.min(1 / p, 1 / q))

  private implicit val rand: Random = new Random()

  def process(neighbor: PSMatrix, iteration: Int, dynamicInitNeighbor: Boolean=false): Node2VecGraphPartition = {
    println(s"partition $index: ---------- iteration $iteration starts ----------")
    println(s"the size of srcNodesArray and the batchSize are ${srcNodesArray.size}, $batchSize.")

    srcNodesArray.indices.sliding(batchSize, batchSize).foreach { nodesIndex =>
      val maybeNeedPullKeyBuf = new LongOpenHashSet(batchSize * 2)
      val flags = new Long2DoubleOpenHashMap(batchSize)
      val pathTail = new Long2ObjectOpenHashMap[Array[Long]](batchSize)
      for (idx <- nodesIndex) {
        val entry = srcNodesSamplePaths(idx)
        val tag = entry(0)
        val Array(src, dst) = entry.slice(entry.size - 2, entry.size)
        val ai = uniform(0.0, upperBound)
        flags.put(tag, ai)
        if (ai > lowerBound) {
          maybeNeedPullKeyBuf.add(src)
        }
        maybeNeedPullKeyBuf.add(dst)
        pathTail.put(tag, Array(src, dst))
      }

      val neighs = if (!maybeNeedPullKeyBuf.isEmpty) {
        val maybeNeedPullKeys = CachedNeigh.set2arr(maybeNeedPullKeyBuf)
        println(s"${Thread.currentThread()}, number of neighs need pull " +
          s"${maybeNeedPullKeys.length}")
        val pulled = Node2VecGraphPartition.getByteNeighborWithTrunc(maybeNeedPullKeys, neighbor, useTrunc, truncLength, dynamicInitNeighbor)
        val neighs = new Long2ObjectOpenHashMap[LongOpenHashSet](pulled.size())
        val iter = pulled.long2ObjectEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          val value = entry.getValue
          val set = new LongOpenHashSet(value.length)
          value.foreach(x => set.add(x))
          neighs.put(entry.getLongKey, set)
        }

        neighs
      } else {
        new Long2ObjectOpenHashMap[LongOpenHashSet]()
      }

      val sampledNodes = rejectSampling(pathTail, neighs, flags)
      for (idx <- nodesIndex) {
        val oldPath = srcNodesSamplePaths(idx)
        val key = srcNodesSamplePaths(idx)(0)
        srcNodesSamplePaths(idx) = Array.concat(oldPath, Array(sampledNodes.get(key)))
      }

    }
    new Node2VecGraphPartition(index, srcNodesArray, srcNodesSamplePaths, batchSize,
      p, q, iteration, useTrunc, truncLength)
  }

  def processWithWeights(alias: PSMatrix, iteration: Int): Node2VecGraphPartition = {
    println(s"partition $index: ---------- iteration $iteration starts ----------")

    srcNodesArray.indices.sliding(batchSize, batchSize).foreach { nodesIndex =>
      val maybeNeedPullKeyBuf = new LongOpenHashSet(batchSize * 2)
      val flags = new Long2DoubleOpenHashMap(batchSize)
      val pathTail = new Long2ObjectOpenHashMap[Array[Long]](batchSize)
      for (idx <- nodesIndex) {
        val entry = srcNodesSamplePaths(idx)
        val tag = entry(0)
        val Array(src, dst) = entry.slice(entry.size - 2, entry.size)
        val ai = uniform(0.0, upperBound)
        flags.put(tag, ai)
        if (ai > lowerBound) {
          maybeNeedPullKeyBuf.add(src)
        }
        maybeNeedPullKeyBuf.add(dst)
        pathTail.put(tag, Array(src, dst))
      }

      val pulledAlias = if (!maybeNeedPullKeyBuf.isEmpty) {
        val maybeNeedPullKeys = CachedNeigh.set2arr(maybeNeedPullKeyBuf)
        println(s"${Thread.currentThread()}, number of neighs need pull " +
          s"${maybeNeedPullKeys.length}")
        val pulled = getAliasTableWithTrunc(maybeNeedPullKeys, alias, false, truncLength)
        pulled
      } else {
        new Long2ObjectOpenHashMap[(Array[Long], Array[Float], Array[Int])]()
      }

      val sampledNodes = biasedRejectSampling(pathTail, pulledAlias, flags)
      for (idx <- nodesIndex) {
        val oldPath = srcNodesSamplePaths(idx)
        val key = srcNodesSamplePaths(idx)(0)
        srcNodesSamplePaths(idx) = Array.concat(oldPath, Array(sampledNodes.get(key)))
      }

    }
    new Node2VecGraphPartition(index, srcNodesArray, srcNodesSamplePaths, batchSize,
      p, q, iteration, useTrunc, truncLength)
  }

  def save(): Array[Array[Long]] = {
    srcNodesSamplePaths
  }

  def pathsClone(): Node2VecGraphPartition = {
    val newSrcNodesArray = srcNodesArray.clone()
    val newPaths = new Array[Array[Long]](srcNodesSamplePaths.length)
    var i = 0
    while (i < srcNodesSamplePaths.length) {
      newPaths(i) = srcNodesSamplePaths(i)
      i += 1
    }
    new Node2VecGraphPartition(index, newSrcNodesArray, newPaths, batchSize, p, q,
      currentIter, useTrunc, truncLength)
  }

  private def uniform(lower: Double, upper: Double): Double = {
    val range = upper - lower
    rand.nextDouble() * range + lower
  }

  protected def getByteNeighborWithTrunc(nodeIds: Array[Long], neighbor: PSMatrix,
                                         useTrunc: Boolean, truncLen: Int):
  Long2ObjectOpenHashMap[Array[Long]] = {
    neighbor.psfGet(
      new GetNeighborWithTrunc(
        new SampleTruncParam(neighbor.id, nodeIds, useTrunc, truncLen)))
      .asInstanceOf[GetLongsResult].getData
  }

  protected def getAliasTableWithTrunc(nodeIds: Array[Long], neighbor: PSMatrix, useTrunc: Boolean,
                                       truncLen: Int):
  Long2ObjectOpenHashMap[(Array[Long], Array[Float], Array[Int])] = {
    neighbor.psfGet(
      new GetAliasTableWithTrunc(
        new SampleTruncParam(neighbor.id, nodeIds, useTrunc, truncLen)))
      .asInstanceOf[PullAliasResult].getResult
  }

  private def rejectSampling(pathTail: Long2ObjectOpenHashMap[Array[Long]],
                             neighs: Long2ObjectOpenHashMap[LongOpenHashSet],
                             flags: Long2DoubleOpenHashMap): Long2LongOpenHashMap = {
    val newPathTail = new Long2LongOpenHashMap(pathTail.size())
    val iter = pathTail.long2ObjectEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val tag = entry.getLongKey
      val Array(src, dst) = entry.getValue

      val dstNeigh = neighs.get(dst)

      var choosed: Long = choice(dstNeigh) // pick up one neighbor randomly
      var ai = flags.get(tag)
      // if ai < lowerBound, then accept
      if (ai >= lowerBound) { // try more
        val srcNeigh = neighs.get(src)

        var isAccept: Boolean = false
        while (!isAccept) {
          isAccept = if (srcNeigh.contains(choosed) && ai < 1.0) { // accept
            true
          } else if (src == choosed && ai < 1 / p) { // accept
            true
          } else if (ai < 1 / q) { // accept
            true
          } else { // reject
            false
          }

          choosed = choice(dstNeigh)
          ai = uniform(0.0, upperBound)
        }
      }

      newPathTail.put(tag, choosed)
    }

    newPathTail
  }

  private def biasedRejectSampling(pathTail: Long2ObjectOpenHashMap[Array[Long]],
                                   pulledAlias: Long2ObjectOpenHashMap[(Array[Long],
                                     Array[Float], Array[Int])],
                                   flags: Long2DoubleOpenHashMap): Long2LongOpenHashMap = {
    val newPathTail = new Long2LongOpenHashMap(pathTail.size())
    val iter = pathTail.long2ObjectEntrySet().fastIterator()
    while (iter.hasNext) {
      val entry = iter.next()
      val tag = entry.getLongKey
      val Array(src, dst) = entry.getValue

      val (dstNeigh, dstAccept, dstAlias) = pulledAlias.get(dst)

      var choosed: Long = biasedChoice(dstNeigh, dstAccept, dstAlias)
      var ai = flags.get(tag)
      // if ai < lowerBound, then accept
      if (ai >= lowerBound) { // try more
        val srcNeigh = pulledAlias.get(src)._1

        var isAccept: Boolean = false
        while (!isAccept) {
          isAccept = if (srcNeigh.contains(choosed) && ai < 1.0) { // accept
            true
          } else if (src == choosed && ai < 1 / p) { // accept
            true
          } else if (ai < 1 / q) { // accept
            true
          } else { // reject
            false
          }

          choosed = biasedChoice(dstNeigh, dstAccept, dstAlias)
          ai = uniform(0.0, upperBound)
        }
      }

      newPathTail.put(tag, choosed)
    }

    newPathTail
  }

  private def choice(neigh: LongOpenHashSet): Long = {
    if (!neigh.isEmpty) {
      val iter = neigh.iterator
      if (neigh.size == 1) {
        iter.hasNext
        iter.nextLong()
      } else {
        iter.skip(rand.nextInt(neigh.size))
        if (iter.hasNext) {
          iter.next()
        } else {
          throw new Exception("Error, please check the dstNeigh !")
        }
      }
    } else {
      throw new Exception("is put is empty!")
    }
  }

  private def biasedChoice(neigh: Array[Long], accept: Array[Float], alias: Array[Int]): Long = {
    if (!neigh.isEmpty && !accept.isEmpty && !alias.isEmpty) {
      if (neigh.size == 1) {
        neigh(0)
      } else {
        val index = Random.nextInt(neigh.size)
        val ac = Random.nextFloat()
        val choosedNode = if (ac < accept(index)) {
          neigh(index)
        } else {
          neigh(alias(index))
        }
        choosedNode
      }
    } else {
      throw new Exception("is put is empty!")
    }
  }

}


object Node2VecGraphPartition {
  protected def getByteNeighborWithTrunc(nodeIds: Array[Long], neighbor: PSMatrix,
                                         useTrunc: Boolean, truncLen: Int, dynamicInitNeighbor: Boolean=false):
  Long2ObjectOpenHashMap[Array[Long]] = {
    neighbor.psfGet(
      new GetNeighborWithTrunc(
        new SampleTruncParam(neighbor.id, nodeIds, useTrunc, truncLen, dynamicInitNeighbor)))
      .asInstanceOf[GetLongsResult].getData
  }

  def initNodePaths(index: Int, psMatrix: PSMatrix, iterator: Iterator[Long], batchSize: Int,
                    p: Double, q: Double, iteration: Int, useTrunc: Boolean, truncLength: Int, dynamicInitNeighbor: Boolean=false):
  Node2VecGraphPartition = {
    val srcNodes = new LongArrayList()
    val dstNodes = new LongArrayList()

    iterator.sliding(batchSize, batchSize).zipWithIndex.foreach { case (nodes_, batchIndex) =>
      val nodes = nodes_.toArray
      val pulled = getByteNeighborWithTrunc(nodes, psMatrix, false, 1, dynamicInitNeighbor)
      val iter = pulled.long2ObjectEntrySet().fastIterator()

      while (iter.hasNext) {
        val entry = iter.next()
        val key = entry.getLongKey
        val value = entry.getValue
        srcNodes.add(key)
        dstNodes.add(value(Random.nextInt(value.length)))
      }
    }

    val srcNodesArray = srcNodes.toLongArray()
    val dstNodesArray = dstNodes.toLongArray()
    val srcNodesSamplePaths = ArrayBuffer[Array[Long]]()
    for (i <- 0 until srcNodesArray.length) {
      srcNodesSamplePaths.append(Array(srcNodesArray(i)) ++ Array(dstNodesArray(i)))
    }

    new Node2VecGraphPartition(index, srcNodesArray, srcNodesSamplePaths.toArray, batchSize,
      p, q, iteration, useTrunc, truncLength)
  }

  def initNodePaths(index: Int, iterator: Iterator[(Long, Array[Long])], batchSize: Int,
                    p: Double, q:Double, iteration: Int, useTrunc: Boolean, truncLength: Int):
  Node2VecGraphPartition = {
    val srcNodes = new LongArrayList()
    val dstNodes = new LongArrayList()
    while (iterator.hasNext) {
      val entry = iterator.next()
      val (src, neighbors) = (entry._1, entry._2)
      srcNodes.add(src)
      dstNodes.add(neighbors(Random.nextInt(neighbors.length)))
    }
    val srcNodesArray = srcNodes.toLongArray()
    val dstNodesArray = dstNodes.toLongArray()
    val srcNodesSamplePaths = ArrayBuffer[Array[Long]]()
    for (i <- 0 until srcNodesArray.length) {
      srcNodesSamplePaths.append(Array(srcNodesArray(i)) ++ Array(dstNodesArray(i)))
    }

    new Node2VecGraphPartition(index, srcNodesArray, srcNodesSamplePaths.toArray, batchSize,
      p, q, iteration, useTrunc, truncLength)
  }

  def initNodePathsWithWeights(index: Int, matrix: PSMatrix,
                               iterator: Iterator[(Long, Array[Long], Array[Float], Array[Int])],
                               batchSize: Int, p: Double, q: Double,
                               iteration: Int, useTrunc: Boolean,
                               truncLength: Int): Node2VecGraphPartition = {
    /*
    val rnd = new Random()
    val srcNodesArray = iterator.toArray.map(_._1)
    val srcNodesSamplePaths = srcNodesArray.map(e => Array(e))
    //sample nodes path batch by batch
    srcNodesArray.indices.sliding(batchSize, batchSize).foreach { nodesIndex =>
      //the tail nodes set of paths
      val pullNodes = srcNodesArray.slice(nodesIndex.head, nodesIndex.last+1)
        .map(a =>  (a, 1)).groupBy(_._1).map(t => (t._1, t._2.size))
      val (nodes, count) = pullNodes.unzip

      //pull nodes neighbors
      val beforeSample = System.currentTimeMillis()
      val nodesToNeighboes = getSampledNeighbors(matrix, nodes.toArray, count.toArray)
      println(s"partition $index, iter $iteration, sampleTime: ${System.currentTimeMillis() - beforeSample} ms")
      println(s"the sizes of nodes and nodesToNeighbors are ${nodes.size}, ${nodesToNeighboes.size()}.")

      //process each node
      for (idx <- nodesIndex) {
        val oldPath = srcNodesSamplePaths(idx) // the old path of idx node
        val oldPathTail = oldPath.last // the tail node of path
        val tailNeighbors = nodesToNeighboes.get(oldPathTail) //the neighbors of tail node
        println(s"oldPathTail: $oldPathTail, size of tailNeighbors: ${tailNeighbors.size}.")
        if (tailNeighbors.nonEmpty) {
          val sampleFromNeighbors = tailNeighbors(rnd.nextInt(tailNeighbors.size)) // sample a node randomly from tail node's neighbors
          srcNodesSamplePaths(idx) = Array.concat(oldPath, Array(sampleFromNeighbors)) // merge old path and sample node
        }

      }
    }
    */
    val srcNodes = new LongArrayList()
    val dstNodes = new LongArrayList()
    while (iterator.hasNext) {
      val entry = iterator.next()
      val (src, neighbors) = (entry._1, entry._2)
      srcNodes.add(src)
      dstNodes.add(neighbors(Random.nextInt(neighbors.length)))
    }
    val srcNodesArray = srcNodes.toLongArray()
    val dstNodesArray = dstNodes.toLongArray()
    val srcNodesSamplePaths = ArrayBuffer[Array[Long]]()
    for (i <- 0 until srcNodesArray.length) {
      srcNodesSamplePaths.append(Array(srcNodesArray(i)) ++ Array(dstNodesArray(i)))
    }

    new Node2VecGraphPartition(index, srcNodesArray, srcNodesSamplePaths.toArray, batchSize,
      p, q, iteration, useTrunc, truncLength)
  }

}