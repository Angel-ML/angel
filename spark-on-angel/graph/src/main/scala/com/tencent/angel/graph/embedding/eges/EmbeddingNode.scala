package com.tencent.angel.graph.embedding.eges

import java.io.{DataInputStream, DataOutputStream, IOException}

import com.tencent.angel.common.{ByteBufSerdeUtils, StreamSerdeUtils}
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

  override def deepClone: Object = {
    val cloneFeats = new Array[Float](feats.length)
    System.arraycopy(feats, 0, cloneFeats, 0, feats.length)
    new EmbeddingNode(feats)
  }

  def this(feats: Array[Float]) {
    this()
    this.feats = feats
  }

  override def serialize(output: ByteBuf): Unit = {
    ByteBufSerdeUtils.serializeFloats(output, feats)
  }

  override def deserialize(input: ByteBuf): Unit = {
    feats = ByteBufSerdeUtils.deserializeFloats(input)
  }

  @throws[IOException]
  override def serialize(output: DataOutputStream): Unit = {
    StreamSerdeUtils.serializeFloats(output, feats)
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

  @throws[IOException]
  override def deserialize(input: DataInputStream): Unit = {
    feats = StreamSerdeUtils.deserializeFloats(input)
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

  override def bufferLen: Int = ByteBufSerdeUtils.serializedFloatsLen(feats)

}