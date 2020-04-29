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

import com.tencent.angel.PartitionKey
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc, UpdateParam}
import com.tencent.angel.ps.storage.partition.RowBasedPartition
import com.tencent.angel.ps.storage.vector.ServerIntAnyRow
import com.tencent.angel.psagent.PSAgentContext
import io.netty.buffer.ByteBuf

import scala.collection.JavaConversions._
import scala.util.Random

/**
  * A PS function to initialize the embedding vector on PS
  *
  * @param param function parameters
  */
class LINEModelRandomize(param: RandomizeUpdateParam) extends UpdateFunc(param) {
  def this() = this(null)

  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    val part = psContext.getMatrixStorageManager.getPart(partParam.getMatrixId,
      partParam.getPartKey.getPartitionId).asInstanceOf[RowBasedPartition]
    if (part != null) {
      val ff = partParam.asInstanceOf[RandomizePartitionUpdateParam]
      update(part, partParam.getPartKey, ff.dim, ff.order, ff.seed)
    }
  }

  private def update(part: RowBasedPartition, key: PartitionKey, dim: Int, order: Int, seed: Int): Unit = {
    val row: ServerIntAnyRow = part.getRow(0).asInstanceOf[ServerIntAnyRow]
    println(s"random seed in init=${seed}")

    val rand = new Random(seed)
    (row.getStartCol until row.getEndCol).foreach(colId => {

      val embedding = new Array[Float](dim)
      for (i <- 0 until dim) {
        embedding(i) = (rand.nextFloat() - 0.5f) / dim
      }
      if (order == 1) {
        row.set(colId.toInt, new LINENode(embedding, null))
      } else {
        row.set(colId.toInt, new LINENode(embedding, new Array[Float](dim)))
      }
    })
  }
}

class RandomizePartitionUpdateParam(matrixId: Int,
                                    partKey: PartitionKey,
                                    var dim: Int,
                                    var order: Int,
                                    var seed: Int)
  extends PartitionUpdateParam(matrixId, partKey) {
  def this() = this(-1, null, -1, -1, -1)

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf)
    buf.writeInt(dim)
    buf.writeInt(order)
    buf.writeInt(seed)
  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)
    this.dim = buf.readInt()
    this.order = buf.readInt()
    this.seed = buf.readInt()
  }

  override def bufferLen: Int = super.bufferLen + 12
}

/**
  * Function parameter
  *
  * @param matrixId embedding matrix id
  * @param dim      embedding vector dim
  * @param order    order
  * @param seed     random seed
  */
class RandomizeUpdateParam(matrixId: Int, dim: Int, order: Int, seed: Int)
  extends UpdateParam(matrixId) {
  override def split: java.util.List[PartitionUpdateParam] = {
    PSAgentContext.get.getMatrixMetaManager.getPartitions(matrixId).map { part =>
      new RandomizePartitionUpdateParam(matrixId, part, dim, order, seed + part.getPartitionId)
    }
  }
}
