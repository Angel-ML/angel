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

import java.nio.charset.StandardCharsets

import com.google.common.base.Throwables
import com.tencent.angel.exception.AngelException
import com.tencent.angel.utils.ByteBufUtils
import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.util.concurrent.{Future, GenericFutureListener}
import org.apache.commons.logging.LogFactory

class RequestMessageHandler(channel: Channel, handler: BusinessMessageHandler) extends MessageHandler[RequestMessage] {
  val LOG = LogFactory.getLog(classOf[RequestMessageHandler])

  override def handler(msg: RequestMessage): Unit = {
    try {
      msg match {
        case msg: CommonRequestMessage => {
          handler.handler(msg.body, new ResponseCallback() {
            override def onSuccess(response: ByteBuf): Unit = {
              respond(new CommonResponseMessage(msg.id, response.readableBytes, response))
            }

            override def onFailure(e: Throwable): Unit = {

              val error = Throwables.getStackTraceAsString(e)
              val bytes = error.getBytes(StandardCharsets.UTF_8)
              val buf = ByteBufUtils.newByteBuf(4 + bytes.length)
              buf.writeInt(bytes.length)
              buf.writeBytes(bytes)
              respond(new FailureMessage(msg.id, 4 + bytes.length, buf))
              LOG.error("handler message failed", e)
            }
          })

        }
        case _ => throw new AngelException("unsupported msg type:" + msg.msgType)
      }
    } finally {
      msg.body.release
    }
  }

  private def respond(result: Encodable): Unit = {
    val remoteAddress = channel.remoteAddress
    channel.writeAndFlush(result).addListener(
      new GenericFutureListener[Future[_ >: Void]]() {
        override def operationComplete(future: Future[_ >: Void]) = {
          if (future.isSuccess) {
            LOG.trace("Sent result {} to client {}", result, remoteAddress)
          }
          else {
            LOG.error(String.format("Error sending result %s to %s closing connection", result, remoteAddress), future.cause)
            channel.close
          }
        }
      })
  }
}
