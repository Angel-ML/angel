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

package com.tencent.angel.transport

import com.tencent.angel.transport.MessageType.MessageType
import io.netty.buffer.ByteBuf

trait Message extends Encodable {
  def msgType: MessageType

  def id: Long

  def body: ByteBuf

  def bodyLen: Int

  final override def encodedLength: Int = 8 + 4

  final override def encode(buf: ByteBuf): Unit = {
    buf.writeLong(id)
    buf.writeInt(bodyLen)
  }
}

trait RequestMessage extends Message {

}

trait ResponseMessage extends Message {

}


object MessageType extends Enumeration {
  type MessageType = Value
  val Request, Response, FAILURE = Value

  def encode(messageType: MessageType, byteBuf: ByteBuf): Unit = {
    require(messageType.id < 127)
    byteBuf.writeByte(messageType.id)
  }

  def decode(byteBuf: ByteBuf): MessageType = {
    val id = byteBuf.readByte().toInt
    MessageType(id)
  }

  def length: Int = 1
}







