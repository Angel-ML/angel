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

import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc, UpdateParam}
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow
import com.tencent.angel.ps.storage.vector.element.LongArrayIntElement


class PushNeighborNumPart(param: UpdateParam) extends UpdateFunc(param) {

  def this() = this(null)

  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    val pparam = partParam.asInstanceOf[PushNeighborNumPartPartitionParam]
    val row = psContext.getMatrixStorageManager.getRow(pparam.getPartKey, 0).asInstanceOf[ServerLongAnyRow]
    val nodeIdToNeighborIndices = pparam.getNodeIdToNeighborIndices
    val iter = nodeIdToNeighborIndices.long2ObjectEntrySet.iterator
    row.startWrite()
    try while (iter.hasNext) {
      val entry = iter.next
      if (entry.getValue._1 == null) {
        //println("neighborSet is null or not: " + (entry.getValue._1 == null))
        //println("the numPart: " + entry.getValue._2)
      }
      row.set(entry.getLongKey, new LongArrayIntElement(entry.getValue._1, entry.getValue._2))
      //println("neighs got from row is null or not: " + (row.get(entry.getLongKey).asInstanceOf[LongArrayIntElement].getData==null))
    }
    finally row.endWrite()
  }
}