package com.tencent.angel.spark.ml.graph.algo.communitydetection.subgraph

import com.tencent.angel.ml.math2.VFactory
import com.tencent.angel.ml.math2.vector.LongLongVector
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.{Long2IntOpenHashMap, LongArrayList}

import scala.collection.mutable.ArrayBuffer

class SeedGraphPartition(index: Int,
                         keys: Array[Long],
                         indptr: Array[Int],
                         neighbors: Array[Long],
                         labels: Array[Long],
                         indices: Array[Long]) extends Serializable {

  def process(model: SeedPSModel, numMsgs: Long, curIter: Int = 1): Long = {
    val inMsgs = model.readMsgs(indices) // read
    val outMsgs = VFactory.sparseLongKeyLongVector(inMsgs.dim())

    if (curIter == 1) {
      for (idx <- keys.indices) {
        if (inMsgs.get(keys(idx)) == 1) { // the key is original seed
          var j = indptr(idx)
          while (j < indptr(idx + 1)) {
            val v = inMsgs.get(neighbors(j))
            if (v == 0) {
              outMsgs.set(neighbors(j), 2)
            }
            j += 1
          }
        }
      }
    } else {
      for (idx <- keys.indices) {
        if (inMsgs.get(keys(idx)) == 2) { // the node is seed or has been targeted
          var j = indptr(idx)
          while (j < indptr(idx + 1)) {
            val v = inMsgs.get(neighbors(j))
            if (v == 0) {
              outMsgs.set(neighbors(j), 3)
            }
            j += 1
          }
        }
      }
    }

    model.initMsgs(outMsgs)
    outMsgs.size()
  }

  /**
    * return result
    *
    * @return nodes ,edges
    */
  def getRes(model: SeedPSModel): (Array[(Long,Int)],Array[(Long,Long)]) = {
    val inMsgs = model.readMsgs(indices) // read
    val nodesRes = new ArrayBuffer[(Long, Int)]()
    val edges = new ArrayBuffer[(Long, Long)]()
    for (idx <- keys.indices) {
      if (inMsgs.get(keys(idx)) > 0) { // the key is original seed
        var j = indptr(idx)
        while (j < indptr(idx + 1)) {
          if (inMsgs.get(neighbors(j)) > 0) {
            if (keys(idx) < neighbors(j)) edges.append((keys(idx),neighbors(j)))
          }
          j += 1
        }
        nodesRes.append((keys(idx), inMsgs.get(keys(idx)).toInt))
      }
    }
    (nodesRes.toArray,edges.toArray)
  }


  def save(): (Array[Long], Array[Long]) =
    (keys, labels)

  def degree(): Array[(Long, Int)] = {
    keys.zipWithIndex.map { case (key, idx) => (key, indptr(idx+1) - indptr(idx)) }
  }
}

object SeedGraphPartition {

  def apply(index: Int, iterator: Iterator[(Long, Iterable[Long])]): SeedGraphPartition = {

    val indptr = new IntArrayList()
    val keys = new LongArrayList()
    val neighbors = new LongArrayList()

    indptr.add(0)
    while (iterator.hasNext) {
      val (nodes, ns) = iterator.next()
      ns.toArray.distinct.foreach(n => neighbors.add(n))
      indptr.add(neighbors.size())
      keys.add(nodes)
    }

    val keysArray = keys.toLongArray()
    val neighborsArray = neighbors.toLongArray()

    new SeedGraphPartition(index, keysArray,
      indptr.toIntArray(), neighborsArray,
      new Array[Long](keysArray.length), keysArray.union(neighborsArray).distinct)
  }
}
