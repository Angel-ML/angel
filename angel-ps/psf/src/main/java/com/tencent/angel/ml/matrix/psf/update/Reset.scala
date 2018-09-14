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


package com.tencent.angel.ml.matrix.psf.update

import java.util

import scala.collection.JavaConversions._
import io.netty.buffer.ByteBuf
import com.tencent.angel.PartitionKey
import com.tencent.angel.ml.matrix.psf.update.Reset.{ZeroParam, ZeroPartitionParam}
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc, UpdateParam}
import com.tencent.angel.psagent.PSAgentContext
import org.apache.commons.logging.{Log, LogFactory}

class Reset(param: ZeroParam) extends UpdateFunc(param) {
  val LOG: Log = LogFactory.getLog(classOf[Reset])
  def this() = this(null)

  def this(matrixId: Int, rowIds: Array[Int]) = this(new ZeroParam(matrixId, rowIds))

  def this(matrixId: Int, rowId: Int) = this(new ZeroParam(matrixId, Array(rowId)))

  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    val part = psContext.getMatrixStorageManager.getPart(partParam.getMatrixId, partParam.getPartKey.getPartitionId)
    if (part != null) {
      partParam.asInstanceOf[ZeroPartitionParam].rowIds.par.foreach { rowId =>
        val row = part.getRow(rowId)
        if(row == null) {
          return
        }
        row.startWrite()
        row.reset()
        row.endWrite()
      }
    }
  }
}

object Reset {

  class ZeroParam(matrixId: Int, rowIds: Array[Int]) extends UpdateParam(matrixId) {
    override def split(): util.List[PartitionUpdateParam] = {
      val partitionKeys = PSAgentContext.get.getMatrixMetaManager.getPartitions(matrixId)
      partitionKeys.map(p =>
        (p, rowIds.filter(rowId => p.getStartRow <= rowId && rowId < p.getEndRow))
      ).filter(_._2.length > 0)
        .map { case (p, ids) =>
          new ZeroPartitionParam(matrixId, p, ids)
        }
    }
  }

  class ZeroPartitionParam(matrixId: Int, partKey: PartitionKey, var rowIds: Array[Int])
    extends PartitionUpdateParam(matrixId, partKey) {
    def this() = this(-1, null, null)

    override def serialize(buf: ByteBuf): Unit = {
      super.serialize(buf)
      buf.writeInt(rowIds.length)
      rowIds.foreach(buf.writeInt)
    }

    override def deserialize(buf: ByteBuf): Unit = {
      super.deserialize(buf)
      this.rowIds = Array.tabulate(buf.readInt)(_ => buf.readInt)
    }
  }

}
