package com.tencent.angel.spark.ml.graph.algo.rank.kcore

import java.util.{Arrays => JArrays}

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.LongIntVector
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList


private[kcore]
class KCoreCompressPartition(index: Int,
                             keys: Array[Long],
                             indptr: Array[Int],
                             neighbors: Array[Long],
                             keyCores: Array[Int],
                             neiCores: Array[Int],
                             hIndices: Array[Int]) extends Serializable {

  def initMsgs(model: KCoreCompressModel, batchSize: Int): Unit = {
    keys.indices.sliding(batchSize, batchSize).foreach { iter =>
      val msgs = VFactory.sparseLongKeyIntVector(model.dim)
      for (i <- iter)
        msgs.set(keys(i), indptr(i + 1) - indptr(i))
      model.initMsgs(msgs)
      println(s"part $index, init ${msgs.size().toInt} msgs.")
    }
  }

  def numEdges(): Long = neighbors.length

  /**
    *
    * @param model
    * @param numMsgs
    * @param iteration
    * @return
    */
  def process(model: KCoreCompressModel, numMsgs: Long, iteration: Int): (KCoreCompressPartition, Int) = {
    val isFirstIteration = iteration == 1
    var minCore = Int.MaxValue
    val nodes2Pull = if (isFirstIteration) keys.union(neighbors).distinct else neighbors.distinct
    if (numMsgs > nodes2Pull.length || isFirstIteration) {
      val beforePull = System.currentTimeMillis()
      val inMsgs = model.readMsgs(nodes2Pull)
      println(s"iter $iteration, partition $index, pulled ${nodes2Pull.length} nodes' msgs, cost ${System.currentTimeMillis() - beforePull} ms.")
      val outMsgs = VFactory.sparseLongKeyIntVector(inMsgs.dim())
      for (idx <- keys.indices) {
        val newIndex = if (isFirstIteration) calcOneFirst(idx, inMsgs) else calcOne(idx, inMsgs)
        if (newIndex < keyCores(idx)) {
          outMsgs.set(keys(idx), newIndex)
          keyCores(idx) = newIndex
          minCore = Math.min(minCore, newIndex)
        }
      }
      println(s"iter $iteration, partition $index, process neighbors size is  " + neighbors.length)

      model.writeMsgs(outMsgs)

      (new KCoreCompressPartition(index, keys, indptr,
        neighbors, keyCores, neiCores, hIndices), minCore)
    } else {
      val beforePull = System.currentTimeMillis()
      val inMsgs = model.readAllMsgs()
      println(s"iter $iteration, partition $index, pulled all nodes' msgs, cost ${System.currentTimeMillis() - beforePull} ms.")
      val outMsgs = VFactory.sparseLongKeyIntVector(inMsgs.dim())
      for (idx <- keys.indices) {
        val newIndex = calcOne(idx, inMsgs)
        if (newIndex < keyCores(idx)) {
          keyCores(idx) = newIndex
          outMsgs.set(keys(idx), newIndex)
          minCore = Math.min(minCore, newIndex)
        }
      }

      model.writeMsgs(outMsgs)
      println(s"iter $iteration, partition $index, process neighbors size is  " + neighbors.length)

      (new KCoreCompressPartition(index, keys, indptr,
        neighbors, keyCores, neiCores, hIndices), minCore)
    }
  }

  def processByBatch(model: KCoreCompressModel, numMsgs: Long, iteration: Int, batchSize: Int): (KCoreCompressPartition, Int) = {
    val isFirstIteration = iteration == 1
    var minCore = Int.MaxValue
//    val nodes2Pull = if (isFirstIteration) keys.union(neighbors).distinct else neighbors.distinct
    if (numMsgs > batchSize * 100 || isFirstIteration) {
      var batchIndex = 0
      keys.indices.sliding(batchSize, batchSize).foreach { iter =>
        val nodes2Pull_ = neighbors.slice(indptr(iter.head), indptr(iter.last + 1))
//        val nodes2Pull_ = if (isFirstIteration) (keys.slice(iter.head, iter.last + 1) ++ neis).distinct else neis.distinct
        val beforePull = System.currentTimeMillis()
        val inMsgs = model.readMsgs(nodes2Pull_)
        println(s"iter $iteration, partition $index, batchIndex $batchIndex, process ${iter.length} keys (total ${keys.length} keys), " +
          s"pulled ${nodes2Pull_.length} nodes' msgs, cost ${System.currentTimeMillis() - beforePull} ms.")
        val outMsgs = VFactory.sparseLongKeyIntVector(inMsgs.dim())
        for (idx <- iter) {
          val newIndex = if (isFirstIteration) calcOneFirst(idx, inMsgs) else calcOne(idx, inMsgs)
          if (newIndex < keyCores(idx)) {
            outMsgs.set(keys(idx), newIndex)
            keyCores(idx) = newIndex
            minCore = Math.min(minCore, newIndex)
          }
        }
        model.writeMsgs(outMsgs)
        outMsgs.clear()
        batchIndex += 1
      }
      println(s"iter $iteration, partition $index, process neighbors size is  " + neighbors.length)

      (new KCoreCompressPartition(index, keys, indptr,
        neighbors, keyCores, neiCores, hIndices), minCore)
    } else {
      val beforePull = System.currentTimeMillis()
      val inMsgs = model.readAllMsgs()
      println(s"iter $iteration, partition $index, pulled all nodes' msgs, cost ${System.currentTimeMillis() - beforePull} ms.")
      val outMsgs = VFactory.sparseLongKeyIntVector(inMsgs.dim())
      for (idx <- keys.indices) {
        val newIndex = calcOne(idx, inMsgs)
        if (newIndex < keyCores(idx)) {
          keyCores(idx) = newIndex
          outMsgs.set(keys(idx), newIndex)
          minCore = Math.min(minCore, newIndex)
        }
      }

      model.writeMsgs(outMsgs)
      println(s"iter $iteration, partition $index ,process neighbors size is  " + neighbors.length)

      (new KCoreCompressPartition(index, keys, indptr,
        neighbors, keyCores, neiCores, hIndices), minCore)
    }
  }

  def compress(model: KCoreCompressModel, numMsgs: Long, minCore: Int = -1, threshold: Int = 0): (KCoreCompressPartition, Int) = {

    val newIndexptr = new IntArrayList()
    val newNeighbors = new LongArrayList()
    val newNeiCores = new IntArrayList()
    newIndexptr.add(0)

    var maxDegree = 0

    for (idx <- keys.indices) {
      if (keyCores(idx) <= minCore || keyCores(idx) <= threshold) {
        newIndexptr.add(newNeighbors.size)
      } else {
        var j = indptr(idx)
        var tmpMaxDegree = 0
        while (j < indptr(idx + 1)) {
          val t = neiCores(j)
          if (t != 0 && t > minCore) {
            newNeighbors.add(neighbors(j))
            newNeiCores.add(t)
            tmpMaxDegree += 1
          }
          j += 1
        }
        maxDegree = math.max(maxDegree, tmpMaxDegree)
        newIndexptr.add(newNeighbors.size)
      }

    }

    val neighboursArray = newNeighbors.toLongArray()
//    println(s"compress neighbors size is  " + neighboursArray.length)
    (new KCoreCompressPartition(index, keys, newIndexptr.toIntArray(), neighboursArray, keyCores, newNeiCores.toIntArray(), new Array[Int](maxDegree)), -1)
  }

  def calcOne(idx: Int, inMsgs: LongIntVector): Int = {
    var j = indptr(idx)
    var flag = false
    while (j < indptr(idx + 1)) {
      val t = inMsgs.get(neighbors(j))
      if (t != 0 && t != neiCores(j)) {
        neiCores(j) = t
        flag = true
      }
      j += 1
    }

    if (flag)
      calcHIndex(neiCores, indptr(idx), indptr(idx + 1))
    else
      keyCores(idx)
  }

  def calcOneFirst(idx: Int, inMsgs: LongIntVector): Int = {
//    keyCores(idx) = inMsgs.get(keys(idx))
    keyCores(idx) = indptr(idx + 1) - indptr(idx)
    var j = indptr(idx)
    while (j < indptr(idx + 1)) {
      neiCores(j) = inMsgs.get(neighbors(j))
      j += 1
    }
    calcHIndex(neiCores, indptr(idx), indptr(idx + 1))
  }

  def calcHIndex(citations: Array[Int], from: Int, to: Int): Int = {
    System.arraycopy(citations, from, hIndices, 0, to - from)
    val start = 0
    val end = to - from
    JArrays.sort(hIndices, 0, end)
    var i = end - 1
    var cnt = 1
    while (i >= start && hIndices(i) >= cnt) {
      cnt += 1
      i -= 1
    }
    cnt - 1
  }

  def save(): (Array[Long], Array[Int]) =
    (keys, keyCores)
}


private[kcore]
object KCoreCompressPartition {
  def apply(index: Int, iterator: Iterator[(Long, Iterable[Long])]): (KCoreCompressPartition, Int) = {
    val indptr = new IntArrayList()
    val keys = new LongArrayList()
    val neighbours = new LongArrayList()

    indptr.add(0)
    var maxDegree: Int = 0
    while (iterator.hasNext) {
      val entry = iterator.next()
      val (node, ns) = (entry._1, entry._2.toArray.distinct)
      ns.foreach(n => neighbours.add(n))
      indptr.add(neighbours.size())
      keys.add(node)
      maxDegree = math.max(ns.size, maxDegree)
    }

    val keysArray = keys.toLongArray()
    val neighboursArray = neighbours.toLongArray()

    (new KCoreCompressPartition(index, keysArray, indptr.toIntArray(),
      neighboursArray, new Array[Int](keysArray.length),
      new Array[Int](neighboursArray.length),
      new Array[Int](maxDegree)), -1)
  }

  def apply(index: Int, keys: Array[Long],
            indptr: Array[Int],
            neighbors: Array[Long],
            keyCores: Array[Int],
            neiCores: Array[Int],
            indices: Array[Long],
            hIndices: Array[Int]): (KCoreCompressPartition, Int) = {
    (new KCoreCompressPartition(index, keys, indptr,
      neighbors, keyCores, neiCores, hIndices), -1)
  }

  def apply1(index: Int, iterator: Iterator[(Long, Array[Long])]): (KCoreCompressPartition, Int) = {
    val indptr = new IntArrayList()
    val keys = new LongArrayList()
    val neighbours = new LongArrayList()

    indptr.add(0)
    var maxDegree: Int = 0
    while (iterator.hasNext) {
      val entry = iterator.next()
      val (node, ns) = (entry._1, entry._2.distinct)
      ns.foreach(n => neighbours.add(n))
      indptr.add(neighbours.size())
      keys.add(node)
      maxDegree = math.max(ns.size, maxDegree)
    }

    val keysArray = keys.toLongArray()
    val neighboursArray = neighbours.toLongArray()

    (new KCoreCompressPartition(index, keysArray, indptr.toIntArray(),
      neighboursArray, new Array[Int](keysArray.length),
      new Array[Int](neighboursArray.length),
      new Array[Int](maxDegree)), -1)
  }

}
