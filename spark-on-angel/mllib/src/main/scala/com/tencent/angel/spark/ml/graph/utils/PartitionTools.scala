package com.tencent.angel.spark.ml.graph.utils

import com.tencent.angel.ml.matrix.{MatrixContext, PartContext}
import org.apache.spark.{Partitioner, SparkPrivateClassProxy}

import scala.collection.JavaConversions._

object PartitionTools {

  def addPartition(ctx: MatrixContext, bounds: Array[Long]): Unit = {
    var lower = Long.MinValue
    var numPart = 0
    for (upper <- bounds ++ Array(Long.MaxValue)) {
      if (lower < upper) {
        ctx.addPart(new PartContext(0, 1, lower, upper, -1))
        lower = upper
        numPart += 1
      }
    }
    println(s"add numPart=$numPart: ${ctx.getParts().map(_.toString).mkString("\n")}")
  }

  def rangePartitionerFromBounds(rangeBounds: Array[Long], ascending: Boolean = true): Partitioner = {

    new Partitioner {
      private val binarySearch: (Array[Long], Long) => Int = SparkPrivateClassProxy.makeBinarySearch[Long]

      override def numPartitions: Int = {
        rangeBounds.length + 1
      }

      override def getPartition(key: Any): Int = {
        val k = key.asInstanceOf[Long]
        var partition = 0
        if (rangeBounds.length <= 128) {
          // If we have less than 128 partitions naive search
          while (partition < rangeBounds.length && k > rangeBounds(partition)) {
            partition += 1
          }
        } else {
          // Determine which binary search method to use only once.
          partition = binarySearch(rangeBounds, k)
          // binarySearch either returns the match location or -[insertion point]-1
          if (partition < 0) {
            partition = -partition - 1
          }
          if (partition > rangeBounds.length) {
            partition = rangeBounds.length
          }
        }
        if (ascending) {
          partition
        } else {
          rangeBounds.length - partition
        }
      }
    }
  }
}
