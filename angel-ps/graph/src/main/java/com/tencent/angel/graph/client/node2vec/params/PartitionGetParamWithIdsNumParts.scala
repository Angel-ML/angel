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
package com.tencent.angel.graph.client.node2vec.params

import com.tencent.angel.PartitionKey
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam
import io.netty.buffer.ByteBuf


class PartitionGetParamWithIdsNumParts(matrixId: Int, partKey: PartitionKey) extends PartitionGetParam(matrixId, partKey) {

  def this() = this(-1, null)

  protected var keyIds: Array[Long] = _
  protected var numParts: Int = _
  protected var startIdx: Int = _
  protected var endIdx: Int = _

  def this(matrixId: Int, partKey: PartitionKey, keyIds: Array[Long], numParts: Int, startIdx: Int, endIdx: Int) {
    this(matrixId, partKey)
    this.keyIds = keyIds
    this.numParts = numParts
    this.startIdx = startIdx
    this.endIdx = endIdx
  }

  def getKeyIds: Array[Long] = keyIds

  def getNumParts: Int = numParts

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf)
    val length = endIdx - startIdx
    buf.writeInt(length)
    for (i <- startIdx until endIdx) {
      buf.writeLong(keyIds(i))
    }
    buf.writeInt(numParts)
  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)
    val length = buf.readInt()
    keyIds = new Array[Long](length)
    startIdx = 0
    endIdx = length
    for (i <- startIdx until endIdx) {
      keyIds(i) = buf.readLong()
    }
    numParts = buf.readInt()
  }

  override def bufferLen: Int = {
    val len = super.bufferLen
    val length = endIdx - startIdx
    len + 4 + 8 * length + 4
  }
}
