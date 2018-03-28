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

import java.io.{Closeable, IOException}

import com.tencent.angel.common.location.Location
import com.tencent.angel.common.transport.ChannelManager
import com.tencent.angel.exception.AngelException
import io.netty.bootstrap.Bootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.channel.{Channel, ChannelInitializer, ChannelOption, EventLoopGroup}
import io.netty.util.concurrent.{Future, GenericFutureListener}
import org.apache.commons.pool.ObjectPool
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try
import scala.util.control.NonFatal


class NetworkClient(val context: NetworkContext) extends Closeable {
  private val LOG: Logger = LoggerFactory.getLogger(classOf[NetworkClient])

  private val conf = context.conf

  /**
    * netty client bootstrap
    */
  private val bootstrap: Bootstrap = new Bootstrap()

  /**
    * netty client thread pool
    */
  private var _eventGroup: EventLoopGroup = _


  /**
    * channel pool manager:it maintain a channel pool for every server
    */
  private val channelManager: ChannelManager = new ChannelManager(bootstrap,
    conf.getInt("serving.client.max.thread.per.location", 5)
  )

  /**
    * use direct netty buffer or not
    */
  private var useDirectBuffer = false

  private val responseMessageHandler: ResponseMessageHandler = ResponseMessageHandler.INSTANCE

  init()

  def init(): Unit = {

    useDirectBuffer = conf.getBoolean("angel.transport.client.use.direct", true)

    val sendBuffSize = conf.getInt("angel.transport.client.send.buf", 1024 * 1024)

    val recvBuffSize = conf.getInt("angel.transport.client.recv.buf", 1024 * 1024)
    _eventGroup = new NioEventLoopGroup(
      conf.getInt("angel.transport.work.num", Runtime.getRuntime.availableProcessors() * 2)
    )
    _eventGroup.asInstanceOf[NioEventLoopGroup].setIoRatio(70)
    bootstrap.group(_eventGroup).channel(classOf[NioSocketChannel])
      .option(ChannelOption.TCP_NODELAY, Boolean.box(true))
      .option(ChannelOption.SO_KEEPALIVE, Boolean.box(true))
      .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, Int.box(60 * 1000))
      .option(ChannelOption.SO_SNDBUF, Int.box(sendBuffSize))
      .option(ChannelOption.SO_RCVBUF, Int.box(recvBuffSize))
      .handler(new ChannelInitializer[SocketChannel]() {
        override def initChannel(ch: SocketChannel): Unit = {
          context.initializePipeline(ch)
        }
      })
  }


  def getChannel(loc: Location): Try[ChannelProxy] = {
    val pool = channelManager.getOrCreateChannelPool(loc)
    Try({
      val channel = pool.borrowObject()
      if (channel == null || !channel.isActive || !channel.isOpen) {
        throw new AngelException(s"$channel not found or invalid")
      }
      new ChannelProxy(channel, pool)
    })
  }

  def sendWithChannel(channelProxy: ChannelProxy, msg: RequestMessage, callback: ResponseCallback) = {
    val startTime = System.currentTimeMillis()
    responseMessageHandler.addCallback(msg.id, callback)

    channelProxy.run((channel: Channel) => {
      channel.writeAndFlush(msg).addListener(new GenericFutureListener[Future[_ >: Void]]() {
        override def operationComplete(future: Future[_ >: Void]) = {
          val remote = channel.remoteAddress
          if (future.isSuccess) {
            val timeTaken: Long = System.currentTimeMillis - startTime
            if (LOG.isTraceEnabled) {
              LOG.trace(s"Sending request $msg.id to $remote took $timeTaken ms")
            }
          } else {
            val cause = future.cause()
            val errorMsg: String = s"Failed to send msg $msg.id to $remote: $cause"
            LOG.error(errorMsg, cause)
            responseMessageHandler.removeCallback(msg.id)

            // why we need close channel here, I think the channel can be reuse
            // channel.close
            try {
              callback.onFailure(new IOException(errorMsg, cause))
            } catch {
              case NonFatal(e) =>
                LOG.error("Uncaught exception in response callback handler!", e)
            }
          }
        }
      })
    })


  }

  def send(loc: Location, msg: RequestMessage, callback: ResponseCallback) = {

    val channelTry = getChannel(loc)
    if (channelTry.isFailure) {
      callback.onFailure(new AngelException(s"$loc failed connect to", channelTry.failed.get))
    } else {
      sendWithChannel(channelTry.get, msg, callback)
    }
  }

  override def close(): Unit = {
    Try {
      bootstrap.config().group().shutdownGracefully
      channelManager.clear()
    }
  }
}


class ChannelProxy(val channel: Channel, val pool: ObjectPool[Channel]) {
  def run(f: Channel => Unit): Unit = {
    try {
      f(channel)
    } finally {
      pool.returnObject(channel)
    }
  }


}
