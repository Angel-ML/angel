/*
 * Tencent is pleased to support the open source community by making Angel available.
 * 
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 * 
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * 
 * https://opensource.org/licenses/BSD-3-Clause
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.ps.matrix.transport;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ps.impl.PSContext;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The Matrix transport server,use netty as network server. This is responsible for client matrix
 * request through {@link MatrixTransportServerHandler} on parameter server.
 *
 * @see com.tencent.angel.psagent.matrix.transport.adapter.MatrixClientAdapter
 * @see com.tencent.angel.psagent.matrix.transport.MatrixTransportClient
 */
public class MatrixTransportServer {
  private static final Log LOG = LogFactory.getLog(MatrixTransportServer.class);
  private final int port;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private ChannelFuture channelFuture;
  private final AtomicBoolean stopped;
  private final PSContext context;

  public MatrixTransportServer(int port, PSContext context) {
    this.port = port;
    this.context = context;
    this.stopped = new AtomicBoolean(false);
  }

  public void start() {
    Configuration conf = context.getConf();
    int workerNum =
        conf.getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_EVENTGROUP_THREADNUM,
            AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_EVENTGROUP_THREADNUM);

    int sendBuffSize =
        conf.getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_SNDBUF,
            AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_SNDBUF);

    int recvBuffSize =
        conf.getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_RCVBUF,
            AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_RCVBUF);

    final int maxMessageSize =
        conf.getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_MAX_MESSAGE_SIZE,
            AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_MAX_MESSAGE_SIZE);

    bossGroup = new NioEventLoopGroup(1);
    workerGroup = new NioEventLoopGroup(workerNum);
    ((NioEventLoopGroup) workerGroup).setIoRatio(70);

    LOG.info("Server port = " + port);

    ServerBootstrap b = new ServerBootstrap();
    b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
        .option(ChannelOption.SO_SNDBUF, sendBuffSize)
        .option(ChannelOption.SO_RCVBUF, recvBuffSize)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline p = ch.pipeline();
            p.addLast(new LengthFieldBasedFrameDecoder(maxMessageSize, 0, 4, 0, 4));
            p.addLast(new LengthFieldPrepender(4));
            p.addLast(new MatrixTransportServerHandler(context));
          }
        });

    channelFuture = b.bind(port);
  }

  public void stop() throws InterruptedException {
    if (!stopped.getAndSet(true)) {
      try {
        if(channelFuture != null) {
          channelFuture.channel().close();
          channelFuture = null;
        }      
      } finally {
        if(bossGroup != null) {
          bossGroup.shutdownGracefully();
          bossGroup = null;
        }
        
        if(workerGroup != null) {
          workerGroup.shutdownGracefully();
        }    
      }
    }
  }
}
