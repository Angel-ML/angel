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

import io.netty.channel.Channel
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.{LengthFieldBasedFrameDecoder, LengthFieldPrepender}
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

class NetworkContext(val conf: Configuration, handler: BusinessMessageHandler = null) {
  val LOG = LogFactory.getLog(classOf[NetworkContext])

  def createServer(host: String): NetworkServer = new NetworkServer(this, host, -1, handler)

  def createClient(): NetworkClient = new NetworkClient(this)


  def initializePipeline(channel: SocketChannel): TransportChannelHandler = initializePipeline(channel, handler)


  def initializePipeline(channel: SocketChannel, handler: BusinessMessageHandler): TransportChannelHandler = try {
    val channelHandler = createChannelHandler(channel, handler)
    channel.pipeline
      .addLast(new LengthFieldBasedFrameDecoder(1024 * 1024 * 1024, 0, 4, 0, 4))
      .addLast(new LengthFieldPrepender(4))
      .addLast("encoder", new MessageEncoder())
      .addLast("decoder", new MessageDecoder())
      .addLast(channelHandler)
    channelHandler
  } catch {
    case e: RuntimeException =>
      LOG.error("Error while initializing Netty pipeline", e)
      throw e
  }


  private def createChannelHandler(channel: Channel, handler: BusinessMessageHandler) = {
    val responseHandler = ResponseMessageHandler.INSTANCE
    val requestHandler = new RequestMessageHandler(channel, handler)
    new TransportChannelHandler(channel, requestHandler, responseHandler)
  }


}
