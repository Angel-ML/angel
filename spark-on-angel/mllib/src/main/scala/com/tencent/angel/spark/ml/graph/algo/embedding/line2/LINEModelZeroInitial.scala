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
package com.tencent.angel.spark.ml.graph.algo.embedding.line2

import com.tencent.angel.PartitionKey
import com.tencent.angel.common.ByteBufSerdeUtils
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc, UpdateParam}
import com.tencent.angel.ps.storage.partition.RowBasedPartition
import com.tencent.angel.ps.storage.vector.ServerIntAnyRow
import com.tencent.angel.psagent.PSAgentContext
import io.netty.buffer.ByteBuf

import scala.collection.convert.ImplicitConversions._

/**
  * A PS function to initialize the embedding vector on PS
  *
  * @param param function parameters
  */
class LINEModelZeroInitial(param: ZeroUpdateParam) extends UpdateFunc(param) {
  def this() = this(null)

  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    val part = psContext.getMatrixStorageManager.getPart(partParam.getMatrixId,
      partParam.getPartKey.getPartitionId).asInstanceOf[RowBasedPartition]
    if (part != null) {
      val ff = partParam.asInstanceOf[ZeroPartitionUpdateParam]
      update(part, partParam.getPartKey, ff.dim, ff.order)
    }
  }

  private def update(part: RowBasedPartition, key: PartitionKey, dim: Int, order: Int): Unit = {
    val row: ServerIntAnyRow = part.getRow(0).asInstanceOf[ServerIntAnyRow]
    (row.getStartCol until row.getEndCol).foreach(colId => {

      val embedding = new Array[Float](dim)
      if (order == 1) {
        row.set(colId.toInt, new LINENode(embedding, null))
      } else {
        row.set(colId.toInt, new LINENode(embedding, new Array[Float](dim)))
      }
    })
  }
}

class ZeroPartitionUpdateParam(matrixId: Int,
                               partKey: PartitionKey,
                               var dim: Int,
                               var order: Int)
  extends PartitionUpdateParam(matrixId, partKey) {
  def this() = this(-1, null, -1, -1)

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf)
    ByteBufSerdeUtils.serializeInt(buf, dim)
    ByteBufSerdeUtils.serializeInt(buf, order)
  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)
    dim = ByteBufSerdeUtils.deserializeInt(buf)
    order = ByteBufSerdeUtils.deserializeInt(buf)
  }

  override def bufferLen: Int = super.bufferLen + ByteBufSerdeUtils.INT_LENGTH * 2
}

/**
  * Function parameter
  *
  * @param matrixId embedding matrix id
  * @param dim      embedding vector dim
  * @param order    order
  */
class ZeroUpdateParam(matrixId: Int, dim: Int, order: Int)
  extends UpdateParam(matrixId) {
  override def split: java.util.List[PartitionUpdateParam] = {
    PSAgentContext.get.getMatrixMetaManager.getPartitions(matrixId).map { part =>
      new ZeroPartitionUpdateParam(matrixId, part, dim, order)
    }
  }
}
