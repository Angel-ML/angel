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

package com.tencent.angel.serving.transport.serving

import com.tencent.angel.utils.ByteBufUtils
import io.netty.buffer.ByteBuf

/**
  * the serving message encoder
  */
object ServingSplitMsgEncoder {

  def encode(msg: ServingSplitMessage[_]): ByteBuf = {
    val buf = ByteBufUtils.newByteBuf(ServingType.length + msg.id.encodedLength + msg.encodedLength)
    // header
    ServingType.encode(msg.msgType, buf)
    msg.id.encode(buf)

    // body
    msg.encode(buf)
    buf
  }
}
