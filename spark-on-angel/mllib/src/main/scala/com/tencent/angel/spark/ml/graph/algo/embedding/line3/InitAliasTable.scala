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

package com.tencent.angel.spark.ml.graph.algo.embedding.line3

import java.util

import com.tencent.angel.PartitionKey
import com.tencent.angel.common.ByteBufSerdeUtils
import com.tencent.angel.graph.utils.GraphMatrixUtils
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc, UpdateParam}
import com.tencent.angel.ps.storage.vector.storage.ElementElementMapStorage
import com.tencent.angel.psagent.PSAgentContext
import io.netty.buffer.ByteBuf

import scala.collection.convert.ImplicitConversions._

/**
  * A PS function that use to build alias table
  *
  * @param param function parameter
  */
class InitAliasTable(param: InitAliasTableParam) extends UpdateFunc(param) {

  def this() = this(null)

  /**
    * Partition update.
    *
    * @param partParam the partition parameter
    */
  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    // Get row from matrix partition that store neighbor table on PS
    val initParam = partParam.asInstanceOf[PartInitAliasTableParam]
    val row = GraphMatrixUtils.getPSAnyKeyRow(psContext, initParam)

    // Get the matrix partition that store alias table on PS
    val aliasTable = psContext.getMatrixStorageManager.getMatrix(initParam.aliasTableId)
    val aliasTablePart = aliasTable.getPartition(initParam.getPartKey.getPartitionId).asInstanceOf[EdgeAliasTablePartition]
    val data = row.getStorage.asInstanceOf[ElementElementMapStorage].getData

    // Get total number of edges
    var edgeNum = 0
    var iter = data.object2ObjectEntrySet().fastIterator()
    while (iter.hasNext) {
      val nodeEntry = iter.next()
      val node = nodeEntry.getValue.asInstanceOf[LINENode]
      if (node.getNeighbors != null) {
        edgeNum += node.getNeighbors.length
      }
    }

    val nodeIds = new Array[ReadOnlyByteArray](data.size())
    val srcNodes = new Array[ReadOnlyByteArray](edgeNum)
    val dstNodes = new Array[ReadOnlyByteArray](edgeNum)
    val weights = new Array[Float](edgeNum)

    var posOffset: Int = 0
    var index = 0
    iter = data.object2ObjectEntrySet().fastIterator()
    while (iter.hasNext) {
      val nodeEntry = iter.next()
      val nodeId = nodeEntry.getKey.asInstanceOf[ReadOnlyByteArray]
      nodeIds(index) = nodeId
      val node = nodeEntry.getValue.asInstanceOf[LINENode]
      if (node.getNeighbors != null) {
        (0 until node.getNeighbors.length).foreach(index => srcNodes(posOffset + index) = nodeId)
        System.arraycopy(node.getNeighbors, 0, dstNodes, posOffset, node.getNeighbors.length)
        System.arraycopy(node.getWeights, 0, weights, posOffset, node.getNeighbors.length)
        posOffset += node.getNeighbors.length
      }
      index += 1
    }

    // Build the alias table
    val storage = new EdgeAliasTableStorage(0, srcNodes, dstNodes, nodeIds, weights)
    storage.buildAliasTable
    aliasTablePart.setStorage(storage)
  }
}

/**
  * Function parameters
  *
  * @param matrixId neighbor table matrix id
  * @param aliasTableId
  */
class InitAliasTableParam(matrixId: Int, aliasTableId: Int) extends UpdateParam(matrixId) {
  /**
    * Split list.
    *
    * @return the list
    */
  override def split(): util.List[PartitionUpdateParam] = {
    val parts = PSAgentContext.get().getMatrixMetaManager.getPartitions(matrixId, 0)
    val partParams = new util.ArrayList[PartitionUpdateParam](parts.size())
    parts.foreach(e => {
      partParams.add(new PartInitAliasTableParam(matrixId, e, aliasTableId))
    })

    partParams
  }
}

class PartInitAliasTableParam(
                               matrixId: Int,
                               partKey: PartitionKey,
                               var aliasTableId: Int) extends PartitionUpdateParam(matrixId, partKey) {

  def this() = this(-1, null, -1)

  override def serialize(output: ByteBuf): Unit = {
    super.serialize(output)
    ByteBufSerdeUtils.serializeInt(output, aliasTableId)
  }

  override def deserialize(input: ByteBuf): Unit = {
    super.deserialize(input)
    aliasTableId = ByteBufSerdeUtils.deserializeInt(input)
  }

  override def bufferLen: Int = {
    super.bufferLen() + ByteBufSerdeUtils.INT_LENGTH
  }
}
