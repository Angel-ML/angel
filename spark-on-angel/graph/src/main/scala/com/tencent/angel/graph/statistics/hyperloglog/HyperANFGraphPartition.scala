package com.tencent.angel.graph.statistics.hyperloglog

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.{Long2ObjectOpenHashMap, LongArrayList}


class HyperANFGraphPartition(index: Int,
                             keys: Array[Long],
                             nodeTags: Array[Int],
                             indptr: Array[Int],
                             outNodes: Array[Long],
                             p: Int, sp: Int, seed: Long) {

  def init(model: HyperANFPSModel): Unit = {
    model.init(keys.clone(), nodeTags.clone(), p, sp, seed)
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

}

object HyperANFGraphPartition {
  def apply(index: Int, iter: Iterator[(Long, Int, Iterable[Long])], p: Int, sp: Int, seed: Long): HyperANFGraphPartition = {
    val indptr = new IntArrayList()
    val outNodes = new LongArrayList()
    val keys = new LongArrayList()
    val nodeTags = new IntArrayList()

    val iterOrder = iter.toArray.sortBy(e => e._1) (Ordering.Long).iterator

    indptr.add(0)
    var idx = 0
    while (iterOrder.hasNext) {
      val entry = iterOrder.next()
      val (node, nodeTag, outs) = (entry._1, entry._2, entry._3)
      outs.toArray.distinct.foreach {n => outNodes.add(n)}
      indptr.add(outNodes.size())
      keys.add(node)
      nodeTags.add(nodeTag)
      idx += 1
    }

    new HyperANFGraphPartition(index, // partition index
      keys.toLongArray(), // node ids
      nodeTags.toIntArray(),
      indptr.toIntArray(), // outNodes sizes
      outNodes.toLongArray(), // neighbor nodes array
      p, sp, seed)
  }

}