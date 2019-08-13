package com.tencent.angel.spark.ml.graph

import it.unimi.dsi.fastutil.longs.LongOpenHashSet

package object data {

  type VertexId = Long

  type PartitionId = Int

  type VertexSet = LongOpenHashSet
}
