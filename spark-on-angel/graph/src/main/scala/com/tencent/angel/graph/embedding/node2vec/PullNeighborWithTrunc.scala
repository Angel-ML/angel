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
package com.tencent.angel.graph.embedding.node2vec

import java.util

import com.tencent.angel.graph.client.node2vec.params.PartitionGetParamWithIds
import com.tencent.angel.graph.client.node2vec.utils.Merge
import com.tencent.angel.ml.matrix.psf.get.base._
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow
import com.tencent.angel.ps.storage.vector.element.LongArrayElement
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap


class PullNeighborWithTrunc(param: GetParam, useTrunc: Boolean, truncLength: Int) extends GetFunc(param) {

  def this() = this(null, false, -1)

  override def partitionGet(partParam: PartitionGetParam): PartitionGetResult = {
    val pparam = partParam.asInstanceOf[PartitionGetParamWithIds]
    val keyIds = pparam.getKeyIds
    val row = psContext.getMatrixStorageManager.getRow(pparam.getPartKey, 0).asInstanceOf[ServerLongAnyRow]
    val partResult = new Long2ObjectOpenHashMap[Array[Long]](keyIds.length)
    if (useTrunc) {
      for (keyId <- keyIds) {
        val longArrayElement = row.get(keyId).asInstanceOf[LongArrayElement]
        val neighs = longArrayElement.getData
        val truncNeigh = if (truncLength < neighs.length) {
          val neighsShuffle = scala.util.Random.shuffle(neighs.toSeq).toArray //没写完，确定可用shuffle函数
          neighsShuffle.slice(0, truncLength)
        } else {
          neighs
        }
        partResult.put(keyId, truncNeigh)
      }
    } else {
      for (keyId <- keyIds) {
        val longArrayElement = row.get(keyId).asInstanceOf[LongArrayElement]
        val neighs = longArrayElement.getData
        partResult.put(keyId, neighs)
      }
    }

    new PullNeighborPartitionResult(partResult)
  }

  override def merge(partResults: util.List[PartitionGetResult]): GetResult = {
    val maps = new util.ArrayList[Long2ObjectOpenHashMap[Array[Long]]](partResults.size)
    import scala.collection.JavaConversions._
    for (partResult <- partResults) {
      maps.add(partResult.asInstanceOf[PullNeighborPartitionResult].getPartResult)
    }
    new PullNeighborResult(Merge.mergeHadhMap(maps))
  }
}
