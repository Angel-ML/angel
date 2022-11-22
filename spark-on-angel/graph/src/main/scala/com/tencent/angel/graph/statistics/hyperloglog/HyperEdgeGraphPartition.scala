package com.tencent.angel.graph.statistics.hyperloglog

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import it.unimi.dsi.fastutil.booleans.BooleanArrayList
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.{Long2ObjectOpenHashMap, LongArrayList}

class HyperEdgeGraphPartition(index: Int,
                              keys: Array[Long],
                              indptr: Array[Int],
                              outNodes: Array[Long],
                              edgeOutNodes: Array[Long],
                              edgeTags: Array[Boolean],
                              p: Int, sp: Int, seed: Long) {

  def init(model: HyperANFPSModel): Unit = {
    model.initEdge((keys ++ outNodes).distinct, p, sp, seed)
  }

  def firstProcess(model: HyperANFPSModel, numBatch: Int): Long = {
    var numMsgs = 0L
    val batchSize = math.max(keys.length / numBatch, 100)
    var start = 0
    while (start < keys.length) {
      numMsgs += batchFirstProcess(model, start, math.min(keys.length, start + batchSize))
      start += batchSize
    }
    numMsgs
  }

  def batchFirstProcess(model: HyperANFPSModel, start: Int, end: Int): Long = {
    if (start == end) return 0
    val outMsgs = new Long2ObjectOpenHashMap[HyperLogLogPlus]()
    for (idx <- start until end) {
      var j = indptr(idx)
      while (j < indptr(idx + 1)) {
        val outNode = outNodes(j)
        val hll = outMsgs.getOrDefault(outNode, new HyperLogLogPlus(p, sp))
        val edgeCounter = new HyperLogLogPlus(p, sp)
        if (edgeTags(j))
          edgeCounter.offerHashed(jenkins(edgeOutNodes(j), seed))
        outMsgs.put(outNode, hll.merge(edgeCounter).asInstanceOf[HyperLogLogPlus])
        j += 1
      }
    }
    model.sendMsgs(outMsgs, p, sp, seed)
    outMsgs.size()
  }

  def process(model: HyperANFPSModel, numBatch: Int): Long = {
    var numMsgs = 0L
    val batchSize = math.max(keys.length / numBatch, 100)
    var start = 0
    while (start < keys.length) {
      numMsgs += batchProcess(model, start, math.min(keys.length, start + batchSize))
      start += batchSize
    }
    numMsgs
  }

  def batchProcess(model: HyperANFPSModel, start: Int, end: Int): Long = {
    if (start == end) return 0
    val outMsgs = new Long2ObjectOpenHashMap[HyperLogLogPlus]()
    val inMsgs = model.getReadCounter(keys.slice(start, end))
    if (inMsgs.size() == 0) return 0
    for (idx <- start until end) {
      if (inMsgs.containsKey(keys(idx))) {
        var j = indptr(idx)
        while (j < indptr(idx + 1)) {
          val outNode = outNodes(j)
          val hll = outMsgs.getOrDefault(outNode, new HyperLogLogPlus(p, sp))
          outMsgs.put(outNode, hll.merge(inMsgs.get(keys(idx))).asInstanceOf[HyperLogLogPlus])
          j += 1
        }
      }
    }
    model.sendMsgs(outMsgs, p, sp, seed)
    outMsgs.size()
  }

  /**
   * Function to compute the hash function from node IDs.
   *
   * Taken from the WebGraph framework, specifically the class
   * IntHyperLogLogCounterArray.
   *
   * Note that the `x` parameter is a `Long`, but the function will also work
   * with `Int` values.
   *
   * @param x    the element to hash, i.e. the node ID
   * @param seed the seed to set up internal state.
   * @return the hashed value of `x`
   */
  private def jenkins(x: Long, seed: Long) = {
    /* Set up the internal state */ var a = seed + x
    var b = seed
    var c = 0x9e3779b97f4a7c13L /* the golden ratio; an arbitrary value */
    a -= b
    a -= c
    a ^= (c >>> 43)
    b -= c
    b -= a
    b ^= (a << 9)
    c -= a
    c -= b
    c ^= (b >>> 8)
    a -= b
    a -= c
    a ^= (c >>> 38)
    b -= c
    b -= a
    b ^= (a << 23)
    c -= a
    c -= b
    c ^= (b >>> 5)
    a -= b
    a -= c
    a ^= (c >>> 35)
    b -= c
    b -= a
    b ^= (a << 49)
    c -= a
    c -= b
    c ^= (b >>> 11)
    a -= b
    a -= c
    a ^= (c >>> 12)
    b -= c
    b -= a
    b ^= (a << 18)
    c -= a
    c -= b
    c ^= (b >>> 22)
    c
  }
}

object HyperEdgeGraphPartition {
  def apply(index: Int, iter: Iterator[(Long, Iterable[(Long, Long, Boolean)])], p: Int, sp: Int, seed: Long):
  HyperEdgeGraphPartition = {
    val indptr = new IntArrayList()
    val outNodes = new LongArrayList()
    val edgeOutNodes = new LongArrayList()
    val edgeTags = new BooleanArrayList()
    val keys = new LongArrayList()

    indptr.add(0)
    var idx = 0
    while (iter.hasNext) {
      val entry = iter.next()
      val (node, outs) = (entry._1, entry._2)
      outs.toArray.distinct.foreach {n => outNodes.add(n._1); edgeOutNodes.add(n._2); edgeTags.add(n._3)}
      indptr.add(outNodes.size())
      keys.add(node)
      idx += 1
    }

    new HyperEdgeGraphPartition(index, // partition index
      keys.toLongArray(), // node ids
      indptr.toIntArray(), // outNodes sizes
      outNodes.toLongArray(), // neighbor nodes array
      edgeOutNodes.toLongArray(),
      edgeTags.toBooleanArray(),
      p, sp, seed)
  }

}