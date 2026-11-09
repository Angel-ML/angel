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
/**
 * Author: zhitaowang (zhitaowang@tecent.com)
 * Created At: 2021/03/01
 */
package com.tencent.angel.spark.ml.graph.algo.embedding.bge

import com.tencent.angel.PartitionKey
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
class BGEModelZeroInitial(param: ZeroUpdateParam) extends UpdateFunc(param) {
  def this() = this(null)

  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    val part = psContext.getMatrixStorageManager.getPart(partParam.getMatrixId,
      partParam.getPartKey.getPartitionId).asInstanceOf[RowBasedPartition]
    if (part != null) {
      val ff = partParam.asInstanceOf[ZeroPartitionUpdateParam]
      update(part, partParam.getPartKey, ff.dim)
    }
  }

  private def update(part: RowBasedPartition, key: PartitionKey, dim: Int): Unit = {
    val row: ServerIntAnyRow = part.getRow(0).asInstanceOf[ServerIntAnyRow]
    (row.getStartCol until row.getEndCol).foreach(colId => {

      val embedding = new Array[Float](dim)
      row.set(colId.toInt, new BGENode(embedding, new Array[Float](dim)))
    })
  }
}

class ZeroPartitionUpdateParam(matrixId: Int,
                                    partKey: PartitionKey,
                                    var dim: Int)
  extends PartitionUpdateParam(matrixId, partKey) {
  def this() = this(-1, null, -1)

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf)
    buf.writeInt(dim)
  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)
    this.dim = buf.readInt()
  }

  override def bufferLen: Int = super.bufferLen + 8
}

/**
  * Function parameter
  *
  * @param matrixId embedding matrix id
  * @param dim      embedding vector dim
  */
class ZeroUpdateParam(matrixId: Int, dim: Int)
  extends UpdateParam(matrixId) {
  override def split: java.util.List[PartitionUpdateParam] = {
    PSAgentContext.get.getMatrixMetaManager.getPartitions(matrixId).map { part =>
      new ZeroPartitionUpdateParam(matrixId, part, dim)
    }
  }
}
