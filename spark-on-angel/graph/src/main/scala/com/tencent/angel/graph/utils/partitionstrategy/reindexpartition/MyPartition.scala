package com.tencent.angel.graph.utils.partitionstrategy.reindexpartition

import java.util

import org.apache.spark.Partitioner

class MyPartition(numParts: Int, rangeBounds: Array[Long]) extends Partitioner {

  override def numPartitions: Int = rangeBounds.length + 1

  private val binarySearch = makeBinarySearch()
  private val ordering = implicitly[Ordering[Long]]

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[Long]
    var partition = 0
    if (rangeBounds.length <= 128) {
      // If we have less than 128 partitions naive search
      while (partition < rangeBounds.length && ordering.gt(k, rangeBounds(partition))) {
        partition += 1
      }
    } else {
      // Determine which binary search method to use only once.
      partition = binarySearch(rangeBounds, k)
      // binarySearch either returns the match location or -[insertion point]-1
      if (partition < 0) {
        partition = -partition-1
      }
      if (partition > rangeBounds.length) {
        partition = rangeBounds.length
      }
    }
      partition
  }

  def makeBinarySearch() : (Array[Long], Long) => Int = {
    (l, x) => util.Arrays.binarySearch(l, x)
  }
}