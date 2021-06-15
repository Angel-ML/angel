package com.tencent.angel.graph.model.neighbor.complex

import com.tencent.angel.ps.storage.vector.element.IElement
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

trait ComplexNeighborOps {
  def initNeighbors(nodeIds: Array[Long], neighbors: Array[IElement])
  def initNeighbors(neighborTable: Seq[(Long, IElement)])
  def getNeighbors(nodeIds: Array[Long]): Long2ObjectOpenHashMap[NeighborsAttrTagElement]
  def sampleNeighbors(nodeIds: Array[Long]): Long2ObjectOpenHashMap[NeighborsAttrTagElement]
}
