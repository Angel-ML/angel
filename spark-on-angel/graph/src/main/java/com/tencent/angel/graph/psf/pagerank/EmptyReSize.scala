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
package com.tencent.angel.graph.psf.pagerank

import java.util

import com.tencent.angel.PartitionKey
import com.tencent.angel.ml.math2.vector.{IntFloatVector, LongFloatVector}
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc, UpdateParam}
import com.tencent.angel.ps.storage.vector.{ServerLongFloatRow, ServerRowUtils}
import com.tencent.angel.psagent.PSAgentContext
import EmptyReSize.{EmptyReSizeParam, EmptyReSizePartParam}
import io.netty.buffer.ByteBuf

import scala.collection.JavaConversions._

class EmptyReSize(param: EmptyReSizeParam) extends UpdateFunc(param) {

  def this() = this(null)
  def this(matrixId: Int, rowId: Array[Int], sizes: Array[Int]) = this(new EmptyReSizeParam(matrixId, rowId, sizes))
  def this(matrixId: Int, rowId: Int, size: Int) = this(new EmptyReSizeParam(matrixId, Array(rowId), Array(size)))

  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    val param = partParam.asInstanceOf[EmptyReSizePartParam]
    param.rowIds.zip(param.sizes).foreach {
      case (rowId, size) =>
        val row = psContext.getMatrixStorageManager.getRow(param.getPartKey, rowId)
        val vector = ServerRowUtils.getVector(row.asInstanceOf[ServerLongFloatRow])
        println(s"rowId=${row.getRowId} vector.size()=${vector.getSize} size=${size}")
        vector match {
          case int: IntFloatVector =>
            int.setStorage(int.getStorage.emptySparse(size))
          case long: LongFloatVector =>
            long.setStorage(long.getStorage.emptySparse(size))
        }
        println(s"rowId=${row.getRowId} vector.size()=${vector.getSize} size=${size}")
    }
  }

}

object EmptyReSize {

  class EmptyReSizeParam(matrixId: Int, rowIds: Array[Int], sizes: Array[Int]) extends UpdateParam(matrixId) {
    override def split(): util.List[PartitionUpdateParam] = {
      val parts = PSAgentContext.get().getMatrixMetaManager.getPartitions(matrixId)
      parts.map(p => new EmptyReSizePartParam(matrixId, p, rowIds, sizes))
    }
  }

  class EmptyReSizePartParam(matrixId: Int, partKey: PartitionKey,
                             var rowIds: Array[Int], var sizes: Array[Int])
    extends PartitionUpdateParam(matrixId, partKey) {

    def this() = this(-1, null, null, null)

    override def serialize(buf: ByteBuf): Unit = {
      super.serialize(buf)
      buf.writeInt(rowIds.length)
      rowIds.foreach(buf.writeInt)
      sizes.foreach(buf.writeInt)
    }

    override def deserialize(buf: ByteBuf): Unit = {
      super.deserialize(buf)
      val len = buf.readInt()
      rowIds = Array.tabulate(len)(_ => buf.readInt)
      sizes = Array.tabulate(len)(_ => buf.readInt)
    }
  }

}