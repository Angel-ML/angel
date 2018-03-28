/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 *  Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *  https://opensource.org/licenses/BSD-3-Clause
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the License
 *  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 *  or implied. See the License for the specific language governing permissions and limitations under
 *  the License.
 *
 */

package com.tencent.angel.serving.common

import java.nio.charset.StandardCharsets

import com.tencent.angel.transport.Encodable
import io.netty.buffer.ByteBuf

/**
  * model split ID
  * @param name the model name
  * @param index the model index
  */
case class ModelSplitID(val name: String, val index: Int) extends Encodable {
  /** Number of bytes of the encoded form of this object. */
  override def encodedLength: Int = name.length * 2 + 4

  /**
    * Serializes this object by writing into the given ByteBuf.
    * This method must write exactly encodedLength() bytes.
    */
  override def encode(buf: ByteBuf): Unit = {
    val bytes = name.getBytes(StandardCharsets.UTF_8)
    buf.writeInt(bytes.length)
    buf.writeBytes(bytes)
    buf.writeInt(index)
  }
}

object ModelSplitID {
  def decode(byteBuf: ByteBuf): ModelSplitID = {
    val nameLen = byteBuf.readInt()
    val bytes = new Array[Byte](nameLen)
    byteBuf.readBytes(bytes)
    val name = new String(bytes, StandardCharsets.UTF_8)
    new ModelSplitID(name, byteBuf.readInt())
  }
}
