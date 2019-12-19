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


package com.tencent.angel.ml.matrix.psf.update.enhance

import com.tencent.angel.PartitionKey
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam
import com.tencent.angel.psagent.PSAgentContext
import io.netty.buffer.ByteBuf
import java.util



/**
 * `FullUpdateParam` is Parameter class for `FullUpdateFunc`
 */
object MultiRowUpdateParam {

  class MultiRowPartitionUpdateParam(matrixId: Int,
      partKey: PartitionKey,
      var rowIds: Array[Int],
      var values: Array[Array[Double]]) extends PartitionUpdateParam(matrixId, partKey) {

    def this() = this(-1, null, null, null)

    override def serialize(buf: ByteBuf): Unit = {
      super.serialize(buf)

      buf.writeInt(rowIds.length)
      for (rowId <- rowIds)
        buf.writeInt(rowId)

      buf.writeInt(values.length)
      for (arr <- values) {
        buf.writeInt(arr.length)
        for (value <- arr)
          buf.writeDouble(value)
      }
    }

    override def deserialize(buf: ByteBuf): Unit = {
      super.deserialize(buf)
      this.rowIds = Array.tabulate(buf.readInt())(_ => buf.readInt())
      this.values = new Array[Array[Double]](buf.readInt())
      for (i <- this.values.indices) {
        this.values(i) = Array.tabulate(buf.readInt())(_ => buf.readDouble())
      }
    }

    override def bufferLen: Int = {
      super.bufferLen + 8 + 4 * this.rowIds.length + this.values.map(_.length).sum * 8 + this.values.length * 4
    }

    def getValues: Array[Array[Double]] = this.values

    def getRowIds: Array[Int] = this.rowIds

    override def toString: String = {
      "FullPartitionUpdateParam values size=" + this.values.length + ", toString()=" + super.toString + "]"
    }
  }

}

class MultiRowUpdateParam(matrixId: Int, val rowIds: Array[Int], val values: Array[Array[Double]])
  extends UpdateParam(matrixId, false) {

  override def split: util.List[PartitionUpdateParam] = {
    val partList = PSAgentContext.get.getMatrixMetaManager.getPartitions(matrixId)
    val size = partList.size
    val partParams = new util.ArrayList[PartitionUpdateParam](size)
    import scala.collection.JavaConversions._
    for (part <- partList) {
      partParams.add(new MultiRowUpdateParam.MultiRowPartitionUpdateParam(matrixId, part, rowIds, values))
    }
    partParams
  }
}
