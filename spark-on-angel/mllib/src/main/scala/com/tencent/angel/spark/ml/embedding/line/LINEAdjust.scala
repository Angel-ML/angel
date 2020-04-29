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
package com.tencent.angel.spark.ml.embedding.line

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

class LINEAdjust(var param: LINEAdjustParam) extends UpdateFunc(param) {

  def this() = this(null)

  /**
    * Partition update.
    *
    * @param partParam the partition parameter
    */
  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    val adjustParam = partParam.asInstanceOf[PartLINEAdjustParam]
    val inputUpdates = adjustParam.inputUpdates
    val outputUpdates = adjustParam.outputUpdates
    val order = adjustParam.order

    val matrix = psContext.getMatrixStorageManager.getMatrix(adjustParam.getMatrixId)
    val part = matrix.getPartition(adjustParam.getPartKey.getPartitionId)
    val row = part.asInstanceOf[RowBasedPartition].getRow(0).asInstanceOf[ServerIntAnyRow]

    //row.startWrite()
    try {
      if (inputUpdates != null) {
        val iter = inputUpdates.int2ObjectEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          inc(row.get(entry.getIntKey).asInstanceOf[LINENode].getInputFeats, entry.getValue)
        }
      }

      if (order == 2 && outputUpdates != null) {
        val iter = outputUpdates.int2ObjectEntrySet().fastIterator()
        while (iter.hasNext) {
          val entry = iter.next()
          inc(row.get(entry.getIntKey).asInstanceOf[LINENode].getOutputFeats, entry.getValue)
        }
      }
    } finally {
      //row.endWrite()
    }
  }

  def inc(dst: Array[Float], src: Array[Float]): Unit = {
    for (i <- dst.indices) {
      dst(i) += src(i)
    }
  }
}

class LINEAdjustParam(matrixId: Int, inputUpdates: Int2ObjectOpenHashMap[Array[Float]],
                      outputUpdates: Int2ObjectOpenHashMap[Array[Float]], order: Int) extends UpdateParam(matrixId) {
  /**
    * Split list.
    *
    * @return the list
    */
  override def split(): util.List[PartitionUpdateParam] = {
    val parts = PSAgentContext.get().getMatrixMetaManager.getPartitions(matrixId, 0)

    // If order == 1, we just need split inputUpdates
    if (order == 1) {
      val nodeIds: Array[Int] = inputUpdates.keySet().toIntArray
      val indicesViews = RowUpdateSplitUtils.split(nodeIds, parts, false)
      val partParams = new util.ArrayList[PartitionUpdateParam](indicesViews.size())

      val iter = indicesViews.entrySet().iterator()
      while (iter.hasNext) {
        val entry = iter.next()
        partParams.add(new PartLINEAdjustParam(matrixId, entry.getKey, order, entry.getValue.getIndices,
          entry.getValue.getStart, entry.getValue.getEnd, inputUpdates, null, -1, -1, null))
      }
      partParams
    } else {
      var partToParams: util.HashMap[PartitionKey, PartitionUpdateParam] = null

      // Split output updaters first
      if (outputUpdates != null && !outputUpdates.isEmpty) {
        val outputNodeIds = outputUpdates.keySet().toIntArray()
        val indicesViews = RowUpdateSplitUtils.split(outputNodeIds, parts, false)
        partToParams = new util.HashMap[PartitionKey, PartitionUpdateParam](indicesViews.size())

        val iter = indicesViews.entrySet().iterator()
        while (iter.hasNext) {
          val entry = iter.next()
          partToParams.put(entry.getKey, new PartLINEAdjustParam(matrixId, entry.getKey, order,
            null, -1, -1, null,
            entry.getValue.getIndices, entry.getValue.getStart, entry.getValue.getEnd, outputUpdates))
        }
      }

      // Merge input update splits
      if (inputUpdates != null && !inputUpdates.isEmpty) {
        val inputNodeIds = inputUpdates.keySet().toIntArray
        val indicesViews = RowUpdateSplitUtils.split(inputNodeIds, parts, false)

        if(partToParams == null) {
          partToParams = new util.HashMap[PartitionKey, PartitionUpdateParam]()
        }

        val iter = indicesViews.entrySet().iterator()
        while (iter.hasNext) {
          val entry = iter.next()
          val partParam = partToParams.get(entry.getKey)
          if (partParam == null) {
            partToParams.put(entry.getKey, new PartLINEAdjustParam(matrixId, entry.getKey, order, entry.getValue.getIndices,
              entry.getValue.getStart, entry.getValue.getEnd, inputUpdates, null, -1, -1, null))
          } else {
            partParam.asInstanceOf[PartLINEAdjustParam].inputUpdates = inputUpdates
            partParam.asInstanceOf[PartLINEAdjustParam].inputNodeIds = entry.getValue.getIndices
            partParam.asInstanceOf[PartLINEAdjustParam].inputStart = entry.getValue.getStart
            partParam.asInstanceOf[PartLINEAdjustParam].intputEnd = entry.getValue.getEnd
          }
        }
      }
      val partParams = new util.ArrayList[PartitionUpdateParam](partToParams.size())
      partParams.addAll(partToParams.values())
      partParams
    }
  }
}

class PartLINEAdjustParam(matrixId: Int, part: PartitionKey, var order: Int, var inputNodeIds: Array[Int], var inputStart: Int, var intputEnd: Int,
                          var inputUpdates: Int2ObjectOpenHashMap[Array[Float]],
                          var outputNodeIds: Array[Int], var outputStart: Int, var outputEnd: Int,
                          var outputUpdates: Int2ObjectOpenHashMap[Array[Float]]) extends PartitionUpdateParam(matrixId, part) {

  def this() = this(-1, null, 1, null, -1, -1, null, null, -1, -1, null)

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf)
    buf.writeInt(order)

    if (inputNodeIds != null) {
      //Size
      buf.writeInt(intputEnd - inputStart)

      // Node grads
      for (i <- inputStart until intputEnd) {
        // Node id
        buf.writeInt(inputNodeIds(i))
        // Node grads
        NodeUtils.serialize(inputUpdates.get(inputNodeIds(i)), buf)
      }
    } else {
      buf.writeInt(0)
    }

    if (outputNodeIds != null) {
      //Size
      buf.writeInt(outputEnd - outputStart)

      // Node grads
      for (i <- outputStart until outputEnd) {
        // Node id
        buf.writeInt(outputNodeIds(i))
        // Node grads
        NodeUtils.serialize(outputUpdates.get(outputNodeIds(i)), buf)
      }
    } else {
      buf.writeInt(0)
    }
  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)

    order = buf.readInt()
    // Node number
    var nodeNum = buf.readInt()
    if (nodeNum > 0) {
      inputUpdates = new Int2ObjectOpenHashMap[Array[Float]](nodeNum)
      for (i <- 0 until nodeNum) {
        inputUpdates.put(buf.readInt(), NodeUtils.deserializeFloats(buf))
      }
    }

    nodeNum = buf.readInt()
    if (nodeNum > 0) {
      outputUpdates = new Int2ObjectOpenHashMap[Array[Float]](nodeNum)
      for (i <- 0 until nodeNum) {
        outputUpdates.put(buf.readInt(), NodeUtils.deserializeFloats(buf))
      }
    }
  }

  override def bufferLen(): Int = {
    var len = super.bufferLen()
    len += 4
    len += 4
    if (inputNodeIds != null && inputNodeIds.nonEmpty) {
      len += (intputEnd - inputStart) * (4 + NodeUtils.dataLen(inputUpdates.get(inputNodeIds(inputStart))))
    }

    if (outputNodeIds != null && outputNodeIds.nonEmpty) {
      len += (outputEnd - outputStart) * (4 + NodeUtils.dataLen(outputUpdates.get(outputNodeIds(outputStart))))
    }

    len
  }
}

