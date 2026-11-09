package com.tencent.angel.spark.ml.graph.algo.statistics.hoplayout

import com.tencent.angel.ml.math2.VFactory
import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList

class SSHopLayoutGraphPartition(index: Int,
                                keys: Array[Long],
                                neighbors: Array[Long],
                                indptr: Array[Int],
                                keyDistance: Array[Int],
                                partNodes: Array[Long]) extends Serializable {
  
  def process(model: SSHopLayoutPSModel): SSHopLayoutGraphPartition = {
    val outMsgs = VFactory.sparseLongKeyIntVector(model.dim)
    val inMsgs = model.readAllMsgs()
    
    for (idx <- keys.indices) {
      if (inMsgs.hasKey(keys(idx))) {
        keyDistance(idx) = math.min(inMsgs.get(keys(idx)), keyDistance(idx))
        val srcDis = keyDistance(idx)
        
        var j = indptr(idx)
        while (j < indptr(idx + 1)) {
          outMsgs.set(neighbors(j), srcDis + 1)
          j += 1
        }
      }
    }
    
    model.writeMsgs(outMsgs)
    
    new SSHopLayoutGraphPartition(index, keys, neighbors, indptr, keyDistance, partNodes)
  }
  
  def leftNodesCount(): Long = {
    var cnt = 0L
    for (dis <- keyDistance) {
      if (dis == Int.MaxValue) {
        cnt += 1
      }
    }
    cnt
  }
  
  def save(): (Array[Long], Array[Int]) = {
    (keys, keyDistance)
  }
  
}

object SSHopLayoutGraphPartition{
  def apply(index: Int, iterator: Iterator[(Long, Iterable[Long])]): SSHopLayoutGraphPartition = {
    val indptr = new IntArrayList()
    val keys = new LongArrayList()
    val keyDis = new IntArrayList()
    val neighbors = new LongArrayList()
  
    indptr.add(0)
  
    while (iterator.hasNext) {
      val (node, ns) = iterator.next()
      keys.add(node)
      keyDis.add(Int.MaxValue)
      ns.toArray.distinct.foreach(n => neighbors.add(n))
      indptr.add(neighbors.size())
    }
  
    val keysArray = keys.toLongArray()
    val neighborsArray = neighbors.toLongArray()
    val keyDisArray = keyDis.toIntArray()
   
    new SSHopLayoutGraphPartition(index, keysArray, neighborsArray,
      indptr.toIntArray, keyDisArray, keysArray.union(neighborsArray).distinct)
    
  }
}
