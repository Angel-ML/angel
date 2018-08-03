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

package com.tencent.angel.transport

import java.util

import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageEncoder

class MessageEncoder extends MessageToMessageEncoder[Message] {


  override def encode(ctx: ChannelHandlerContext, msg: Message, out: util.List[AnyRef]): Unit = {
    val msgType = msg.msgType
    val headerLen = msg.encodedLength + MessageType.length
    val header = ctx.alloc.heapBuffer(headerLen)
    MessageType.encode(msgType, header)
    msg.encode(header)
    out.add(Unpooled.wrappedBuffer(header,msg.body))
    //out.add(new ComposedMessage(header, msg.body, msg.bodyLen))
  }
}

object MessageEncoder {
  val INSTANCE = new MessageEncoder()

}
