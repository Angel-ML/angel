package com.tencent.angel.graph.utils.element

import it.unimi.dsi.fastutil.longs.LongOpenHashSet

object Element {
  type VertexId = Long

  type PartitionId = Int

  type VertexSet = LongOpenHashSet

  type CounterTriangleDirected = Array[Int]
}
