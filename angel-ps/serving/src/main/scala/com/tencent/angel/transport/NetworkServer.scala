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

import java.io.Closeable
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.utils.NetUtils
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{ChannelFuture, ChannelInitializer, ChannelOption}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory

import scala.util.Try

class NetworkServer(val context: NetworkContext, val hostToBind: String, portToBind: Int = -1, val businessHandler: BusinessMessageHandler) extends Closeable {
  private val LOG = LoggerFactory.getLogger(classOf[NetworkServer])

  val conf: Configuration = context.conf

  {
    try init()
    catch {
      case e: RuntimeException =>
        IOUtils.closeQuietly(this)
        throw e
    }
  }

  private var bootstrap: ServerBootstrap = _
  private var channelFuture: ChannelFuture = _
  private[this] var _port: Int = _

  private def port: Int = _port

  def getPort: Int = {
    if (port == -1) throw new IllegalStateException("Server not initialized")
    port
  }

  private def init() = {

    val workerNum = conf.getInt(AngelConf.ANGEL_NETTY_SERVING_TRANSFER_SERVER_EVENTGROUP_THREADNUM,
      AngelConf.DEFAULT_ANGEL_NETTY_SERVING_TRANSFER_SERVER_EVENTGROUP_THREADNUM)
    val bossGroup = new NioEventLoopGroup(1)
    val workerGroup = new NioEventLoopGroup(workerNum)
    workerGroup.asInstanceOf[NioEventLoopGroup].setIoRatio(70)
    bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])
      //.childOption(ChannelOption.SO_BACKLOG, Int.box(conf.getInt("angel.transport.backlog", 1024)))
      .childOption(ChannelOption.SO_RCVBUF, Int.box(conf.getInt("angel.transport.revbuf", 1024 * 64)))
      .childOption(ChannelOption.SO_SNDBUF, Int.box(conf.getInt("angel.transport.sendbuf", 1024 * 256)))
      .childHandler(new ChannelInitializer[SocketChannel]() {
        @throws[Exception]
        override protected def initChannel(ch: SocketChannel): Unit = {
          val rpcHandler = businessHandler
          context.initializePipeline(ch, rpcHandler)
        }
      })
    if (portToBind == -1) {
      (0 to 10).find(_ => {
        Try(
          {
            val searchPort = NetUtils.chooseAListenPort(conf)
            val address = if (hostToBind == null) new InetSocketAddress(searchPort)
            else new InetSocketAddress(hostToBind, searchPort)
            channelFuture = bootstrap.bind(address)
          }
        ).isSuccess
      })
    } else {
      val address = if (hostToBind == null) new InetSocketAddress(portToBind)
      else new InetSocketAddress(hostToBind, portToBind)
      channelFuture = bootstrap.bind(address)
    }
    channelFuture.syncUninterruptibly()
    _port = channelFuture.channel.localAddress.asInstanceOf[InetSocketAddress].getPort
    LOG.debug("Server started on port: {}", port)
  }

  override def close(): Unit = {
    if (channelFuture != null) { // close is a local operation and should finish within milliseconds timeout just to be safe
      channelFuture.channel.close.awaitUninterruptibly(10, TimeUnit.SECONDS)
      channelFuture = null
    }
    if (bootstrap != null) {
      bootstrap.config().group().shutdownGracefully
      bootstrap.config().childGroup().shutdownGracefully
      bootstrap = null
    }
  }
}
