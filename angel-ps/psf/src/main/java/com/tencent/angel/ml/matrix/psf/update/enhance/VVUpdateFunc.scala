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

import scala.collection.JavaConversions._

import io.netty.buffer.ByteBuf

import com.tencent.angel.ml.math2.vector.Vector
import com.tencent.angel.ml.matrix.psf.RowUpdateWithVectorUtils
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc, UpdateParam}
import com.tencent.angel.ml.matrix.psf.update.enhance.VVUpdateFunc.{VVUpdateParam, VVUpdatePartitionParam}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.psagent.matrix.transport.adapter.RowsViewUpdateItem

abstract class VVUpdateFunc(param: UpdateParam) extends UpdateFunc(param) {
  def this(matrixId: Int, rowId: Int, vector: Vector) = this(new VVUpdateParam(matrixId, rowId, vector))

  def this() = this(null)

  override def partitionUpdate(partParam: PartitionUpdateParam) {
    val part = psContext.getMatrixStorageManager.getPart(partParam.getMatrixId, partParam.getPartKey.getPartitionId)

    if (part != null) {
      val va = partParam.asInstanceOf[VVUpdatePartitionParam]
      val buf = va.getBuf
      RowUpdateWithVectorUtils.update(part, buf, updateDouble, updateFloat, updateLong, updateInt)
    }
  }

  def updateDouble(self: Double, other: Double): Double

  def updateFloat(self: Float, other: Float): Float

  def updateLong(self: Long, other: Long): Long

  def updateInt(self: Int, other: Int): Int
}

object VVUpdateFunc{
  class VVUpdateParam(matrixId:Int, rowId:Int, vector: Vector) extends UpdateParam(matrixId) {
    override def split(): java.util.List[PartitionUpdateParam] = {
      val colNum = PSAgentContext.get.getMatrixMetaManager.getMatrixMeta(matrixId).getColNum
      val partitions = PSAgentContext.get.getMatrixMetaManager.getPartitions(matrixId)
      partitions.map{ partKey =>
        val item = new RowsViewUpdateItem(partKey, Array(vector), colNum)
        new VVUpdatePartitionParam(item, matrixId)
      }
    }
  }
  
  class VVUpdatePartitionParam(item: RowsViewUpdateItem, matrixId: Int) 
    extends PartitionUpdateParam(matrixId, item.getPartKey){
    var buf:ByteBuf = _
    def getBuf:ByteBuf = buf
    
    override def serialize(buf: ByteBuf): Unit = {
      super.serialize(buf)
      item.serialize(buf)
    }

    override def deserialize(buf: ByteBuf): Unit = {
      super.deserialize(buf)
      this.buf = buf
    }

    override def bufferLen(): Int = super.bufferLen() + item.bufferLen()
  }
}
