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

import java.nio.charset.StandardCharsets

import com.tencent.angel.transport.MessageType.MessageType
import io.netty.buffer.ByteBuf

class FailureMessage(val id: Long, val bodyLen: Int, error: ByteBuf) extends ResponseMessage {
  override def msgType: MessageType = MessageType.FAILURE

  override def body: ByteBuf = error
}

object FailureMessage {
  def decode(buf: ByteBuf): FailureMessage = {
    new FailureMessage(buf.readLong(), buf.readInt, buf.retain())
  }

  def toError(msg: FailureMessage): String = {
    val bytes = new Array[Byte](msg.bodyLen)
    msg.body.readBytes(bytes)
    new String(bytes, StandardCharsets.UTF_8)
  }
}
