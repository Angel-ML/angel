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
package com.tencent.angel.graph.client.node2vec.getfuncs.pullneighbor

import java.util

import com.tencent.angel.graph.client.node2vec.params.PartitionGetParamWithIds
import com.tencent.angel.graph.client.node2vec.utils.Merge
import com.tencent.angel.ml.matrix.psf.get.base._
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow
import com.tencent.angel.ps.storage.vector.element.LongArrayIntElement
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap


class PullNumPart(param: GetParam) extends GetFunc(param) {

  def this() = this(null)

  override def partitionGet(partParam: PartitionGetParam): PartitionGetResult = {
    val pparam = partParam.asInstanceOf[PartitionGetParamWithIds]
    val keyIds = pparam.getKeyIds
    val row = psContext.getMatrixStorageManager.getRow(pparam.getPartKey, 0).asInstanceOf[ServerLongAnyRow]
    val partResult = new Long2IntOpenHashMap(keyIds.length)
    for (keyId <- keyIds) {
      val longArrayIntElement = row.get(keyId).asInstanceOf[LongArrayIntElement]
      val numPart = longArrayIntElement.getPartNum
      partResult.put(keyId, numPart)
    }
    //println("partResult in pullNumPart is null or not: " + (partResult == null))
    new PullNumPartPartitionResult(partResult)
  }

  override def merge(partResults: util.List[PartitionGetResult]): GetResult = {
    val maps = new util.ArrayList[Long2IntOpenHashMap](partResults.size)
    import scala.collection.convert.ImplicitConversions._
    for (partResult <- partResults) {
      maps.add(partResult.asInstanceOf[PullNumPartPartitionResult].getPartResult)
    }
    new PullNumPartResult(Merge.mergeHashMap(maps))
  }
}
