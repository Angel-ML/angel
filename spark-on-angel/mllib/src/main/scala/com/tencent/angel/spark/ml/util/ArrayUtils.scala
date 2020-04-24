package com.tencent.angel.spark.ml.util

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object ArrayUtils {
  /**
    * @param srcEdges sorted by nodeId
    * @param dstEdges sorted by nodeId
    * @tparam ED type of attribute on edge
    * @return (nodeId, (src_tag, dst_tag))
    */
  def intersect[ED: ClassTag](srcEdges: Array[(Long, ED)], dstEdges: Array[(Long, ED)]): Array[(Long, (ED, ED))] = {

    val res = new ArrayBuffer[(Long, (ED, ED))]

    if (srcEdges == null || dstEdges == null || srcEdges.length == 0 || dstEdges.length == 0)
      return Array()

    var i = 0
    var j = 0
    while (i < srcEdges.length && j < dstEdges.length) {
      if (srcEdges(i)._1 < dstEdges(j)._1) i += 1
      else if (srcEdges(i)._1 > dstEdges(j)._1) j += 1
      else {
        res += ((srcEdges(i)._1, (srcEdges(i)._2, dstEdges(j)._2)))
        i += 1
        j += 1
      }
    }
    res.toArray
  }
}
