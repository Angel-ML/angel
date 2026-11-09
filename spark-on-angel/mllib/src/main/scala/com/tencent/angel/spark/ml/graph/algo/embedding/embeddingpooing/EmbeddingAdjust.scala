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

package com.tencent.angel.spark.ml.graph.algo.embedding.embeddingpooing

import java.util

import com.tencent.angel.PartitionKey
import com.tencent.angel.graph.data.NodeUtils
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc, UpdateParam}
import com.tencent.angel.ps.storage.partition.RowBasedPartition
import com.tencent.angel.ps.storage.vector.ServerIntAnyRow
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils
import io.netty.buffer.ByteBuf
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap

class EmbeddingAdjust(var param: EmbeddingAdjustParam) extends UpdateFunc(param) {

  def this() = this(null)

  /**
    * Partition update.
    *
    * @param partParam the partition parameter
    */
  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    val adjustParam = partParam.asInstanceOf[PartEmbeddingAdjustParam]
    val inputUpdates = adjustParam.inputUpdates

    val matrix = psContext.getMatrixStorageManager.getMatrix(adjustParam.getMatrixId)
    val part = matrix.getPartition(adjustParam.getPartKey.getPartitionId)
    val row = part.asInstanceOf[RowBasedPartition].getRow(0).asInstanceOf[ServerIntAnyRow]

    //row.startWrite()
    try {
      if (inputUpdates != null) {
        val iter = inputUpdates.int2ObjectEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          apy(row.get(entry.getIntKey).asInstanceOf[EmbeddingNode].getFeats, entry.getValue)
        }
      }
    } finally {
      //row.endWrite()
    }
  }

  def apy(dst: Array[Float], src: Array[Float]): Unit = {
    for (i <- dst.indices) {
      dst(i) = src(i)
    }
  }
}

class EmbeddingAdjustParam(matrixId: Int, inputUpdates: Int2ObjectOpenHashMap[Array[Float]]) extends UpdateParam(matrixId) {
  /**
    * Split list.
    *
    * @return the list
    */
  override def split(): util.List[PartitionUpdateParam] = {
    val parts = PSAgentContext.get().getMatrixMetaManager.getPartitions(matrixId, 0)
    val nodeIds: Array[Int] = inputUpdates.keySet().toIntArray
    val indicesViews = RowUpdateSplitUtils.split(nodeIds, parts, false)
    val partParams = new util.ArrayList[PartitionUpdateParam](indicesViews.size())

    val iter = indicesViews.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      partParams.add(new PartEmbeddingAdjustParam(matrixId, entry.getKey, entry.getValue.getIndices,
        entry.getValue.getStart, entry.getValue.getEnd, inputUpdates))
    }
    partParams
  }
}

class PartEmbeddingAdjustParam(matrixId: Int, part: PartitionKey, var inputNodeIds: Array[Int], var inputStart: Int, var inputEnd: Int,
                               var inputUpdates: Int2ObjectOpenHashMap[Array[Float]]) extends PartitionUpdateParam(matrixId, part) {

  def this() = this(-1, null, null, -1, -1, null)

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf)

    if (inputNodeIds != null) {
      //Size
      buf.writeInt(inputEnd - inputStart)

      // Node Embedding
      for (i <- inputStart until inputEnd) {
        // Node id
        buf.writeInt(inputNodeIds(i))
        // Node Embedding
        NodeUtils.serializeShortFromFloat(inputUpdates.get(inputNodeIds(i)), buf)
      }
    } else {
      buf.writeInt(0)
    }
  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)

    // Node number
    val nodeNum = buf.readInt()
    if (nodeNum > 0) {
      inputUpdates = new Int2ObjectOpenHashMap[Array[Float]](nodeNum)
      for (_ <- 0 until nodeNum) {
        inputUpdates.put(buf.readInt(), NodeUtils.deserializeFloatFromShort(buf))
      }
    }
  }

  override def bufferLen(): Int = {
    var len = super.bufferLen()
    len += 4
    if (inputNodeIds != null && inputNodeIds.nonEmpty) {
      len += (inputEnd - inputStart) * (4 + NodeUtils.dataLenShortFromFloat(inputUpdates.get(inputNodeIds(inputStart))))
    }
    len
  }
}
