package com.tencent.angel.graph.model.neighbor.complex

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

object ComplexNeighborUtils {
  def flatNeighbors(neighbors: Long2ObjectOpenHashMap[NeighborsAttrTagElement]): Long2ObjectOpenHashMap[Array[(Long, Byte, Float)]] = {
    val flatedNeighbors = new Long2ObjectOpenHashMap[Array[(Long, Byte, Float)]](neighbors.size())

    val iter = neighbors.long2ObjectEntrySet().fastIterator()
    while (iter.hasNext) {
      val neighborEntry = iter.next()
      val elem = neighborEntry.getValue
      val arr = new Array[(Long, Byte, Float)](elem.getNodesNum)
      val edges: Array[Long] = elem.getNeighborIds
      val attrs: Array[Float] = elem.getAttrs
      val tags: Array[Byte] = elem.getTags

      for (i <- 0 until elem.getNodesNum) {
        arr(i) = ((edges(i), tags(i), attrs(i)))
      }
      flatedNeighbors.put(neighborEntry.getLongKey, arr)
    }

    flatedNeighbors
  }
}
