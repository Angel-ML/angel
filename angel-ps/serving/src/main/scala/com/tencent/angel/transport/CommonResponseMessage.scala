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

class CommonResponseMessage(val id: Long, val bodyLen: Int, val body: ByteBuf) extends ResponseMessage {

  def this(id: Long, body: ByteBuf) {
    this(id, body.readableBytes(), body)
  }

  override def msgType: MessageType = MessageType.Response
}

object CommonResponseMessage {


  def decode(byteBuf: ByteBuf): CommonResponseMessage = {
    new CommonResponseMessage(byteBuf.readLong(), byteBuf.readInt(), byteBuf.retain())
  }
}
