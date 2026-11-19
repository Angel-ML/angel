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
package com.tencent.angel.graph.client.node2vec.updatefuncs.pushneighbor

import com.tencent.angel.PartitionKey
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam
import io.netty.buffer.ByteBuf
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap


class PushNeighborNumPartPartitionParam(matrixId: Int, partKey: PartitionKey, updateClock: Boolean)
  extends PartitionUpdateParam(matrixId, partKey, updateClock) {

  def this() = this(-1, null, false)

  var keyIds: Array[Long] = _
  var startIdx: Int = _
  var endIdx: Int = _
  var nodeIdToNeighborIndices: Long2ObjectOpenHashMap[(Array[Array[Long]], Int)] = _

  def this(matrixId: Int, partKey: PartitionKey, updateClock: Boolean, nodeIdToNeighborIndices:
  Long2ObjectOpenHashMap[(Array[Array[Long]], Int)], keyIds: Array[Long], startIdx: Int, endIdx: Int) {
    this(matrixId, partKey, updateClock = false)
    this.keyIds = keyIds
    this.startIdx = startIdx
    this.endIdx = endIdx
    this.nodeIdToNeighborIndices = nodeIdToNeighborIndices
  }

  def getNodeIdToNeighborIndices: Long2ObjectOpenHashMap[(Array[Array[Long]], Int)] = nodeIdToNeighborIndices

  protected def clear(): Unit = {
    keyIds = null
    startIdx = -1
    endIdx = -1
    nodeIdToNeighborIndices = null
  }

  override def serialize(buf: ByteBuf): Unit = {
    super.serialize(buf) // serialize matrixId, updateClock and partKey
    //.serLong2ArrayHashMap(keyIds, startIdx, endIdx, nodeIdToNeighborIndices, buf)
    //println("the values of starIdx and endIdx:" + startIdx + ", " + endIdx)
    val size = endIdx - startIdx
    if (size <= 0) {
      buf.writeInt(0)
    } else {
      buf.writeInt(size)
      for (i <- startIdx until endIdx) {
        val key = keyIds(i)
        buf.writeLong(key)
        val neighbor = nodeIdToNeighborIndices.get(key)._1
        val numPart = nodeIdToNeighborIndices.get(key)._2
        if (neighbor == null) {
          buf.writeInt(0)
        } else {
          buf.writeInt(numPart)
          for (j <- 0 until numPart) {
            buf.writeInt(neighbor(j).length)
            for (k <- 0 until neighbor(j).length) {
              buf.writeLong(neighbor(j)(k))
            }
          }
        }
      }
    }
    //clear()
  }

  override def deserialize(buf: ByteBuf): Unit = {
    super.deserialize(buf)
    //nodeIdToNeighborIndices = SerDe.deserLong2LongArray(buf)
    val size = buf.readInt()
    if (size <= 0) {
      nodeIdToNeighborIndices = null
    } else {
      nodeIdToNeighborIndices = new Long2ObjectOpenHashMap[(Array[Array[Long]], Int)](size)
      for (i <- 0 until size) {
        val key = buf.readLong()
        val numPart = buf.readInt()
        val neigh: Array[Array[Long]] = if (numPart <= 0) {
          //neigh = null
          //println("numPart is not greater than 0 causes neighs in deserialization is null: " + numPart + ", true")
          null
        } else {
          val neigh = new Array[Array[Long]](numPart)
          for (j <- 0 until numPart) {
            val len = buf.readInt()
            val neighbor = new Array[Long](len)
            for (k <- 0 until len) {
              neighbor(k) = buf.readLong()
            }
            neigh(j) = neighbor
          }
          neigh
        }
        nodeIdToNeighborIndices.put(key, (neigh, numPart))
        //println("the neighs got from deserialization in partitionParam is null or not: " + (nodeIdToNeighborIndices.get(key)._1==null))
      }
    }
  }

  override def bufferLen: Int = {
    var len = super.bufferLen
    val size = endIdx - startIdx
    if (size <= 0) {
      len += 4
    } else {
      len += (4 + size * (8 + 4)) //size, key, numPart
      for (i <- startIdx until endIdx) {
        val (neigh, numPart) = nodeIdToNeighborIndices.get(keyIds(i))
        if (neigh != null && numPart >= 1) {
          len += 4 * numPart // length of all parts
          for (j <- 0 until numPart) {
            len += 8 * neigh(j).length
          }
        }
      }
    }
    len
  }
}
