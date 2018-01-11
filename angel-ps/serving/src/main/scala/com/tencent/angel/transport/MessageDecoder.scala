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

import java.util

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageDecoder

class MessageDecoder extends MessageToMessageDecoder[ByteBuf] {
  override def decode(ctx: ChannelHandlerContext, msg: ByteBuf, out: util.List[Object]) = {
    val msgType = MessageType.decode(msg)
    val decoded = msgType match {
      case MessageType.Request => CommonRequestMessage.decode(msg)
      case MessageType.Response => CommonResponseMessage.decode(msg)
      case MessageType.FAILURE => FailureMessage.decode(msg)
      case _ => msg
    }

    out.add(decoded)
  }
}
