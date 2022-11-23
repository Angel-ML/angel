package com.tencent.angel.graph.statistics.hyperloglog

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.{Long2ObjectOpenHashMap, LongArrayList}
import scala.collection.mutable.ArrayBuffer

class HyperDecimalGraphPartition(index: Int,
                                 keys: Array[Long],
                                 indptr: Array[Int],
                                 outNodes: Array[Long],
                                 infos: Array[Long],
                                 edgeTags: Array[Int],
                                 rangeIntervals: Array[(Long, Long)],
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
        val (rangeStart, rangeEnd) = rangeIntervals(j)
        val mergedCounter = new HyperLogLogPlus(p, sp)
        for (i <- rangeStart until rangeEnd) {
          val hashed = jenkins(i, seed)
          mergedCounter.offerHashed(hashed)
        }
        val hll = outMsgs.getOrDefault(outNode, new HyperLogLogPlus(p, sp))
        outMsgs.put(outNode, hll.merge(mergedCounter).asInstanceOf[HyperLogLogPlus])
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
        while (j < indptr(idx + 1)) { //
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

  private def mergeCounter(hll1: HyperLogLogPlus, hll2: HyperLogLogPlus): HyperLogLogPlus = {
    hll1.merge(hll2).asInstanceOf[HyperLogLogPlus]
  }
}

object HyperDecimalGraphPartition {
  def apply(index: Int, iter: Iterator[(Long, Iterable[(Long, Long, Int, (Long, Long))])], p: Int, sp: Int, seed: Long):
  HyperDecimalGraphPartition = {
    val indptr = new IntArrayList()
    val outNodes = new LongArrayList()
    val keys = new LongArrayList()
    val infos = new LongArrayList()
    val edgeTags = new IntArrayList()
    val rangeIntervals = new ArrayBuffer[(Long, Long)]()

    indptr.add(0)
    var idx = 0
    while (iter.hasNext) {
      val entry = iter.next()
      val (node, outs) = (entry._1, entry._2)
      outs.toArray.distinct.foreach{e => outNodes.add(e._1); infos.add(e._2); edgeTags.add(e._3);
        rangeIntervals.append(e._4);}
      indptr.add(outNodes.size())
      keys.add(node)
      idx += 1
    }

    new HyperDecimalGraphPartition(index, // partition index
      keys.toLongArray(), // node ids
      indptr.toIntArray(), // outNodes sizes
      outNodes.toLongArray(), // neighbor nodes array
      infos.toLongArray(),
      edgeTags.toIntArray(),
      rangeIntervals.toArray,
      p, sp, seed)
  }

}