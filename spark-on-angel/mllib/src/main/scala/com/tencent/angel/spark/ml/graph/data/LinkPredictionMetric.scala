/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
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
