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
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap


class PullAliasWithTrunc(param: GetParam, useTrunc: Boolean, truncLength: Int) extends GetFunc(param) {

  def this() = this(null, false, -1)

  override def partitionGet(partParam: PartitionGetParam): PartitionGetResult = {
    val pparam = partParam.asInstanceOf[PartitionGetParamWithIds]
    val keyIds = pparam.getKeyIds
    val row = psContext.getMatrixStorageManager.getRow(pparam.getPartKey, 0).asInstanceOf[ServerLongAnyRow]
    val partResult = new Long2ObjectOpenHashMap[(Array[Long], Array[Float], Array[Int])](keyIds.length)
    if (useTrunc) {
      for (keyId <- keyIds) {
        val aliasElement = row.get(keyId).asInstanceOf[AliasElement]
        val neighs = aliasElement.getNeighborIds
        val accepts = aliasElement.getAccept
        val alias = aliasElement.getAlias
        val entries = neighs.zip(accepts).zip(alias).map(e => (e._1._1, e._1._2, e._2))
        val truncEntries = if (truncLength < neighs.length) {
          val entriesShuffle = scala.util.Random.shuffle(entries.toSeq).toArray
          entriesShuffle.slice(0, truncLength)
        } else {
          entries
        }
        partResult.put(keyId, truncEntries.unzip3)
      }
    } else {
      for (keyId <- keyIds) {
        val aliasElement = row.get(keyId).asInstanceOf[AliasElement]
        val neighs = aliasElement.getNeighborIds
        val accepts = aliasElement.getAccept
        val alias = aliasElement.getAlias
        partResult.put(keyId, (neighs, accepts, alias))
      }
    }

    new PullAliasPartitionResult(partResult)
  }

  override def merge(partResults: util.List[PartitionGetResult]): GetResult = {
    val maps = new util.ArrayList[Long2ObjectOpenHashMap[(Array[Long], Array[Float], Array[Int])]](partResults.size)
    import scala.collection.JavaConversions._
    for (partResult <- partResults) {
      maps.add(partResult.asInstanceOf[PullAliasPartitionResult].getPartResult)
    }
    new PullAliasResult(Merge.mergeHadhMap(maps))
  }
}
