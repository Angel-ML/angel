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

package com.tencent.angel.spark.ml.graph.algo.embedding.embeddingpooing

import java.io.{DataInputStream, DataOutputStream, IOException}

import com.tencent.angel.ps.storage.vector.element.IElement
import io.netty.buffer.ByteBuf

/**
  * A user-define data type that store embedding node information on PS
  */
class EmbeddingNode extends IElement {
  private var feats: Array[Float] = _

  def getFeats: Array[Float] = feats

  def setFeats(feats: Array[Float]): Unit = {
    this.feats = feats
  }

  def this(feats: Array[Float]) {
    this()
    this.feats = feats
  }

  override def deepClone: Object = {
    val cloneFeats = new Array[Float](feats.length)
    System.arraycopy(feats, 0, cloneFeats, 0, feats.length)
    new EmbeddingNode(feats)
  }

  override def serialize(output: ByteBuf): Unit = {
    output.writeInt(feats.length)
    for (i <- feats.indices) {
      output.writeFloat(feats(i))
    }
  }

  override def deserialize(input: ByteBuf): Unit = {
    feats = new Array[Float](input.readInt)
    for (i <- feats.indices) {
      feats(i) = input.readFloat
    }
  }

  @throws[IOException]
  override def serialize(output: DataOutputStream): Unit = {
    val data = new Array[Byte](bufferLen)
    var index = 0
    output.writeInt(data.length)
    index = writeInt(data, feats.length, index)
    for (i <- feats.indices) {
      index = writeFloat(data, feats(i), index)
    }
    output.write(data)
  }

  @throws[IOException]
  override def deserialize(input: DataInputStream): Unit = {
    val data = new Array[Byte](input.readInt)
    input.readFully(data)
    var index = 0
    val featLen = readInt(data, index)
    feats = new Array[Float](featLen)
    index += 4
    for (i <- 0 until featLen) {
      feats(i) = readFloat(data, index)
      index += 4
    }
  }

  private def writeInt(data: Array[Byte], v: Int, index: Int) = {
    data(index) = ((v >>> 24) & 0xFF).toByte
    data(index + 1) = ((v >>> 16) & 0xFF).toByte
    data(index + 2) = ((v >>> 8) & 0xFF).toByte
    data(index + 3) = ((v >>> 0) & 0xFF).toByte
    index + 4
  }

  private def writeFloat(data: Array[Byte], v: Float, index: Int) = {
    val iv = java.lang.Float.floatToIntBits(v)
    writeInt(data, iv, index)
  }

  private def readInt(data: Array[Byte], index: Int) = {
    val ch1 = data(index) & 255
    val ch2 = data(index + 1) & 255
    val ch3 = data(index + 2) & 255
    val ch4 = data(index + 3) & 255
    val c = (ch1 << 24) + (ch2 << 16) + (ch3 << 8) + ch4
    c
  }

  private def readFloat(data: Array[Byte], index: Int) = java.lang.Float.intBitsToFloat(readInt(data, index))

  override def dataLen: Int = bufferLen

  override def bufferLen: Int = 4 + feats.length * 4

}
