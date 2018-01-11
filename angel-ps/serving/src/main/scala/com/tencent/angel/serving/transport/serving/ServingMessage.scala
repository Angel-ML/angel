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

import com.tencent.angel.serving.common.ModelSplitID
import com.tencent.angel.serving.transport.serving.ServingType.ServingType
import com.tencent.angel.transport.Encodable
import io.netty.buffer.ByteBuf

/**
  * the trait of serving message, the message type and model split ID is header will encode and decode
  * on transfer framework
  */
trait ServingMessage extends Encodable {
}

trait ServingSplitMessage[T] extends ServingMessage {
  def msgType: ServingType

  def id: ModelSplitID

  def body: T
}

trait ServingSplitReqMsg[T] extends ServingSplitMessage[T] {

}

trait ServingSplitResMsg[T] extends ServingSplitMessage[T] {

}


object ServingType extends Enumeration {
  type ServingType = Value

  val BATCH_PREDICT_REQUEST, BATCH_PREDICT_RESPONSE, PREDICT_REQUEST, PREDICT_RESPONSE = Value

  def encode(seringType: ServingType, byteBuf: ByteBuf): Unit = {
    require(seringType.id < 127)
    byteBuf.writeByte(seringType.id)
  }

  def decode(byteBuf: ByteBuf): ServingType = {
    val id = byteBuf.readByte().toInt
    ServingType(id)
  }

  def length: Int = 1
}
