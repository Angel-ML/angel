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

import io.netty.buffer.ByteBuf

import com.tencent.angel.PartitionKey
import com.tencent.angel.ml.matrix.psf.update.base.{PartitionUpdateParam, UpdateFunc, UpdateParam}
import com.tencent.angel.psagent.PSAgentContext
import com.tencent.angel.spark.ml.psf.embedding.NENSTableInitializer._

class NENSTableInitializer(params: UpdateParam) extends UpdateFunc(params) {
  def this() = this(null)

  def this(matrixId: Int, maxIndex: Int, seed: Int, version: Int)
  = this(new NENSTableInitializerParam(matrixId, maxIndex, seed, version))

  override def partitionUpdate(partParam: PartitionUpdateParam): Unit = {
    val part = psContext.getMatrixStorageManager.getPart(partParam.getMatrixId, partParam.getPartKey.getPartitionId)
    if (part != null) {
      val p = partParam.asInstanceOf[NENSTableInitializerPartitionParam]
      NENegativeSample.init(p.maxIndex, p.seed, p.version)
    }
  }
}

object NENSTableInitializer {

  class NENSTableInitializerPartitionParam(matrixId: Int,
                                           partKey: PartitionKey,
                                           var maxIndex: Int,
                                           var seed: Int,
                                           var version: Int)
    extends PartitionUpdateParam(matrixId, partKey) {
    def this() = this(-1, null, -1, -1, -1)

    override def serialize(buf: ByteBuf): Unit = {
      super.serialize(buf)
      buf.writeInt(maxIndex)
      buf.writeInt(seed)
      buf.writeInt(version)
    }

    override def deserialize(buf: ByteBuf): Unit = {
      super.deserialize(buf)
      this.maxIndex = buf.readInt()
      this.seed = buf.readInt()
      this.version = buf.readInt()
    }

    override def bufferLen(): Int = super.bufferLen() + 8
  }

  class NENSTableInitializerParam(matrixId: Int, maxIndex: Int, seed: Int, version: Int) extends UpdateParam(matrixId) {
    override def split(): java.util.List[PartitionUpdateParam] = {
      PSAgentContext.get.getMatrixMetaManager.getPartitions(matrixId)
        .map(new NENSTableInitializerPartitionParam(matrixId, _, maxIndex, seed, version))
    }
  }

}
