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
package com.tencent.angel.graph.client.node2vec.getfuncs.pullneighbor

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult
import io.netty.buffer.ByteBuf
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap


class PullNumPartPartitionResult extends PartitionGetResult {

  private var partResult: Long2IntOpenHashMap = null

  def this(part: Long2IntOpenHashMap) {
    this()
    this.partResult = part
  }

  def getPartResult: Long2IntOpenHashMap = partResult

  override def serialize(output: ByteBuf): Unit = {
    output.writeInt(partResult.size)
    if (partResult.size > 0) {
      val keySet = partResult.keySet
      for (i <- keySet.toLongArray) {
        output.writeLong(i)
        output.writeInt(partResult.get(i))
      }
    }
  }

  override def deserialize(input: ByteBuf): Unit = {
    val size = input.readInt()
    if (size > 0) {
      partResult = new Long2IntOpenHashMap(size)
      for (i <- 0 until size) {
        partResult.put(input.readLong(), input.readInt())
      }
    } else {
      partResult = null
    }
    //println("partResult in deserialization is null or not: " + (partResult == null))
  }

  override def bufferLen: Int = 4 + partResult.size * (8 + 4)
}
