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
package com.tencent.angel.graph.client.node2vec.updatefuncs.pushneighbor

import java.util

import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateParam}
import com.tencent.angel.psagent.PSAgentContext
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap


class PushNeighborNumPartParam(matrixId: Int, updateClock: Boolean) extends UpdateParam(matrixId, updateClock) {

  def this() = this(-1, false)

  private var keyIds: Array[Long] = null
  var nodeIdToNeighborIndices: Long2ObjectOpenHashMap[(Array[Array[Long]], Int)] = null

  def this(matrixId: Int, updateClock: Boolean, adjTable: Long2ObjectOpenHashMap[(Array[Array[Long]], Int)]) {
    this(matrixId, false)
    this.nodeIdToNeighborIndices = adjTable
    this.keyIds = adjTable.keySet().toLongArray
    util.Arrays.sort(keyIds)
    /*
    for(e <- keyIds) {
      println("neighs in PushNeighborNumPartParam is null or not: " + (nodeIdToNeighborIndices.get(e)._1==null))
    }
    */
  }

  override def split: util.List[PartitionUpdateParam] = {
    val parts = PSAgentContext.get.getMatrixMetaManager.getPartitions(matrixId)
    val partParams = new util.ArrayList[PartitionUpdateParam](parts.size) // parts参数Array列表
    var count = 0
    var nodeIndex = 0
    import scala.collection.convert.ImplicitConversions._
    for (part <- parts) {
      val start = nodeIndex // include start
      while (nodeIndex < keyIds.length && keyIds(nodeIndex) < part.getEndCol) {
        //start, end指的是整体element的范围，与element内部分割无关
        nodeIndex += 1
      }
      val end = nodeIndex // exclude end
      val sizePart = end - start
      count += sizePart
      if (sizePart > 0) {
        partParams.add(new PushNeighborNumPartPartitionParam(matrixId, part, false, nodeIdToNeighborIndices, keyIds, start, end))
      }
    }
    assert(count == nodeIdToNeighborIndices.size)
    partParams
  }
}
