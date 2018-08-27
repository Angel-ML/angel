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


package com.tencent.angel.spark.ml.psf.embedding

import scala.collection.JavaConversions._
import scala.util.Random

import io.netty.buffer.ByteBuf

import com.tencent.angel.PartitionKey
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc, UpdateParam}
import com.tencent.angel.ps.storage.matrix.ServerPartition
import com.tencent.angel.ps.storage.vector.ServerIntFloatRow
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.psf.embedding.NEModelRandomize.{RandomizePartitionUpdateParam, RandomizeUpdateParam}

/**
  * initialize ps matrix with Uniform distribution U(-1/(2*dim), 1/(2*dim))
  */
class NEModelRandomize(param: RandomizeUpdateParam) extends UpdateFunc(param) {
  def this(matrixId: Int, partDim: Int, dimension: Int, order: Int, seed: Int) =
    this(new RandomizeUpdateParam(matrixId, partDim, dimension, order, seed))

  def this() = this(null)

  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    val part = psContext.getMatrixStorageManager.getPart(partParam.getMatrixId, partParam.getPartKey.getPartitionId)
    if (part != null) {
      val ff = partParam.asInstanceOf[RandomizePartitionUpdateParam]
      update(part, partParam.getPartKey, ff.partDim, ff.dim, ff.order, ff.seed)
    }
  }

  private def update(part: ServerPartition, key: PartitionKey, partDim: Int, dim: Int, order: Int, seed: Int): Unit = {
    val startRow = key.getStartRow
    val endRow = key.getEndRow
    val rand = new Random(seed)
    (startRow until endRow).map(rowId => (rowId, rand.nextInt)).par.foreach { case (rowId, rowSeed) =>
      val rowRandom = new Random(rowSeed)
      val data = part.getRow(rowId).asInstanceOf[ServerIntFloatRow].getValues
      if (order == 1)
        data.indices.foreach(data(_) = (rowRandom.nextFloat() - 0.5f) / dim)
      else {
        val nodeOccupied = 2 * partDim
        data.indices.foreach(i =>
          data(i) = if (i % nodeOccupied < partDim) (rowRandom.nextFloat() - 0.5f) / dim else 0.0f)
      }
    }
  }
}

object NEModelRandomize {

  class RandomizePartitionUpdateParam(matrixId: Int,
                                      partKey: PartitionKey,
                                      var partDim: Int,
                                      var dim: Int,
                                      var order: Int,
                                      var seed: Int)
    extends PartitionUpdateParam(matrixId, partKey) {
    def this() = this(-1, null, -1, -1, -1, -1)

    override def serialize(buf: ByteBuf): Unit = {
      super.serialize(buf)
      buf.writeInt(partDim)
      buf.writeInt(dim)
      buf.writeInt(order)
      buf.writeInt(seed)
    }

    override def deserialize(buf: ByteBuf): Unit = {
      super.deserialize(buf)
      this.partDim = buf.readInt()
      this.dim = buf.readInt()
      this.order = buf.readInt()
      this.seed = buf.readInt()
    }

    override def bufferLen: Int = super.bufferLen + 16
  }


  class RandomizeUpdateParam(matrixId: Int, partDim: Int, dim: Int, order: Int, seed: Int)
    extends UpdateParam(matrixId) {
    override def split: java.util.List[PartitionUpdateParam] = {
      PSAgentContext.get.getMatrixMetaManager.getPartitions(matrixId).map { part =>
        new RandomizePartitionUpdateParam(matrixId, part, partDim, dim, order, seed + part.getPartitionId)
      }
    }
  }

}
