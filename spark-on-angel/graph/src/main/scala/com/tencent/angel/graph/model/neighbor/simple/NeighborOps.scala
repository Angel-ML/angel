package com.tencent.angel.graph.model.neighbor.simple

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

trait NeighborOps {
  def initNeighbors(nodeIds: Array[Long], neighbors: Array[Array[Long]])
  def initNeighbors(neighborTable: Seq[(Long, Array[Long])])
  def getNeighbors(nodeIds: Array[Long]): Long2ObjectOpenHashMap[Array[Long]]
  def sampleNeighbors(nodeIds: Array[Long]): Long2ObjectOpenHashMap[Array[Long]]
}
