package com.tencent.angel.graph.reindex

import java.io.{DataInputStream, DataOutputStream}
import java.util

import com.tencent.angel.common.{ByteBufSerdeUtils, StreamSerdeUtils}
import com.tencent.angel.ps.storage.vector.element.IElement
import io.netty.buffer.ByteBuf

class ReadOnlyByteArray(var data: Array[Byte]) extends Serializable with IElement {
  @volatile
  var hashcode = util.Arrays.hashCode(data)

  def this() = this(null)

  override def hashCode(): Int = hashcode

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ReadOnlyByteArray => {
        util.Arrays.equals(data, that.data)
      }

      case _ => {
        false
      }
    }
  }

  override def deepClone(): AnyRef = new ReadOnlyByteArray(data.clone())

  /**
    * Serialize object to the Output stream.
    *
    * @param output the Netty ByteBuf
    */
  override def serialize(output: ByteBuf): Unit = {
    ByteBufSerdeUtils.serializeInt(output, hashcode)
    ByteBufSerdeUtils.serializeBytes(output, data)
  }

  /**
    * Deserialize object from the input stream.
    *
    * @param input the input stream
    */
  override def deserialize(input: ByteBuf): Unit = {
    hashcode = ByteBufSerdeUtils.deserializeInt(input)
    data = ByteBufSerdeUtils.deserializeBytes(input)
  }

  /**
    * Estimate serialized data size of the object, it used to ByteBuf allocation.
    *
    * @return int serialized data size of the object
    */
  override def bufferLen(): Int = {
    ByteBufSerdeUtils.INT_LENGTH + ByteBufSerdeUtils.serializedBytesLen(data)
  }

  /**
    * Serialize object to the Output stream.
    *
    * @param output the Netty ByteBuf
    */
  override def serialize(output: DataOutputStream): Unit = {
    StreamSerdeUtils.serializeInt(output, hashcode)
    StreamSerdeUtils.serializeBytes(output, data)
  }

  /**
    * Deserialize object from the input stream.
    *
    * @param input the input stream
    */
  override def deserialize(input: DataInputStream): Unit = {
    hashcode = StreamSerdeUtils.deserializeInt(input)
    data = StreamSerdeUtils.deserializeBytes(input)
  }

  override def dataLen(): Int = bufferLen()

  override def toString = s"ReadOnlyByteArray($hashcode, ${new String(data)})"
}
