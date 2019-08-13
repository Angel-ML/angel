package com.tencent.angel.spark.ml.graph.data

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap

case class LinkPredictionMetric(
                                commonFriends: Long,
                                totalFriends: Long,
                                preferAttachment: Long,
                                adamicAdar: Double,
                                resourceAllocation: Double
                                ) {

  override def toString: String = {
    s"common friends = $commonFriends, " +
      s"total friends = $totalFriends, " +
      s"adamic adar = $adamicAdar, " +
      s"preferential attachment = $preferAttachment, " +
      s"resource allocation = $resourceAllocation"
  }
}

object LinkPredictionMetric {

  def getMetric(data: Long2ObjectOpenHashMap[Array[Long]],
                srcId: Long,
                dstId: Long): LinkPredictionMetric = {
    val srcNeighbors = data.get(srcId)
    val dstNeighbors = data.get(dstId)
    val intersectArr = srcNeighbors intersect dstNeighbors
    val unionArr = srcNeighbors union dstNeighbors
    val commonFriends = intersectArr.length
    val totalFriends = unionArr.length
    val prederAttachment = srcNeighbors.length * dstNeighbors.length
    val adamicAdar = intersectArr.map( t => 1.0 / math.log(data.get(t).length)).sum
    val resourceAllocation = intersectArr.map( t => 1.0 / data.get(t).length).sum
    LinkPredictionMetric(commonFriends, totalFriends, prederAttachment, adamicAdar, resourceAllocation)
  }
}
