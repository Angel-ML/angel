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


package com.tencent.angel.ps.server.data;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.utils.NetUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The Matrix transport server,use netty as network server. This is responsible for client matrix
 * request through {@link MatrixTransportServerHandler} on parameter server.
 *
 * @see com.tencent.angel.psagent.matrix.transport.adapter.UserRequestAdapter
 * @see com.tencent.angel.psagent.matrix.transport.MatrixTransportClient
 */
public class MatrixTransportServer {
  private static final Log LOG = LogFactory.getLog(MatrixTransportServer.class);

  /**
   * How many times we retry the (protocol-fixed) data port bind on "address already in use"
   */
  private static final int MAX_BIND_TRY = 3;
  private static final long BIND_RETRY_INTERVAL_MS = 500;

  private final int port;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private ChannelFuture channelFuture;
  private final AtomicBoolean stopped;
  private final PSContext context;

  /**
   * Reservation holding this data port since PS startup; released right before the real bind
   */
  private volatile NetUtils.PortReservation portReservation;

  public MatrixTransportServer(int port, PSContext context) {
    this.port = port;
    this.context = context;
    this.stopped = new AtomicBoolean(false);
  }

  /**
   * Hand over the reservation that currently holds this data port. It is released right
   * before {@link #start()} binds the port, which keeps other processes from stealing the
   * port during the (potentially multi-second) PS startup.
   */
  public void setPortReservation(NetUtils.PortReservation portReservation) {
    this.portReservation = portReservation;
  }

  public void start() {
    Configuration conf = context.getConf();
    int workerNum = conf.getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_EVENTGROUP_THREADNUM,
      AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_EVENTGROUP_THREADNUM);

    int sendBuffSize = conf.getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_SNDBUF,
      AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_SNDBUF);

    int recvBuffSize = conf.getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_RCVBUF,
      AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_RCVBUF);

    final int maxMessageSize = conf.getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_MAX_MESSAGE_SIZE,
      AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_MAX_MESSAGE_SIZE);

    int ioRatio = conf.getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_IORATIO,
      AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_IORATIO);

    String channelType = conf.get(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_CHANNEL_TYPE,
      AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_CHANNEL_TYPE);
    boolean autoEpoll = conf.getBoolean(
      AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_CHANNEL_AUTO_EPOLL,
      AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_CHANNEL_AUTO_EPOLL);
    int backlog = conf.getInt(
      AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_BACKLOG,
      AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_BACKLOG);
    boolean tcpNoDelay = conf.getBoolean(
      AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_TCP_NODELAY,
      AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_TCP_NODELAY);
    int writeHigh = conf.getInt(
      AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_WRITE_BUFFER_HIGH_WATERMARK,
      AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_WRITE_BUFFER_HIGH_WATERMARK);
    int writeLow = conf.getInt(
      AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_WRITE_BUFFER_LOW_WATERMARK,
      AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_WRITE_BUFFER_LOW_WATERMARK);
    if (writeLow >= writeHigh) {
      LOG.warn("write.buffer.low.watermark (" + writeLow + ") >= high (" + writeHigh
          + "); falling back to high/4 to avoid an invalid WriteBufferWaterMark.");
      writeLow = Math.max(1, writeHigh / 4);
    }

    // Idle timeout for server-side channels. When a channel sees neither read nor write for
    // this many seconds we proactively close it so that the client's connection pool refreshes
    // a dead long-lived TCP session before any intermediate NAT / firewall silently drops it.
    // See AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_CHANNEL_IDLE_SECONDS for rationale.
    final int channelIdleSeconds = conf.getInt(
      AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_CHANNEL_IDLE_SECONDS,
      AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_CHANNEL_IDLE_SECONDS);

    // Channel selection: Epoll on Linux when either explicitly requested OR when auto-epoll
    // is enabled and the netty epoll native transport is loadable. Falls back to NIO on
    // non-Linux or if epoll bootstrap fails (e.g. missing libc / glibc mismatch). The NIO
    // path now also honours the configured ioRatio (was hardcoded to 70 historically –
    // making the server.ioratio knob silently ineffective on non-epoll runs).
    Class channelClass;
    String os = System.getProperty("os.name");
    boolean isLinux = os != null && os.toLowerCase().startsWith("linux");
    boolean wantEpoll = isLinux
        && ("epoll".equalsIgnoreCase(channelType) || (autoEpoll && Epoll.isAvailable()));

    if (wantEpoll) {
      LOG.info("Use epoll channel (channelType=" + channelType
          + ", autoEpoll=" + autoEpoll + ", Epoll.isAvailable=" + Epoll.isAvailable() + ")");
      channelClass = EpollServerSocketChannel.class;
      bossGroup = new EpollEventLoopGroup(1);
      workerGroup = new EpollEventLoopGroup(workerNum);
      ((EpollEventLoopGroup) workerGroup).setIoRatio(ioRatio);
    } else {
      if (isLinux && autoEpoll && !Epoll.isAvailable()) {
        // Operator likely expected epoll on a Linux box but the native lib failed to load.
        // Surface the diagnostic so it is investigatable in the PS log instead of silently
        // running on the slower NIO path.
        LOG.warn("auto-epoll enabled on Linux but Epoll.isAvailable()=false; reason: "
            + Epoll.unavailabilityCause(), Epoll.unavailabilityCause());
      }
      LOG.info("Use nio channel (channelType=" + channelType + ", isLinux=" + isLinux + ")");
      channelClass = NioServerSocketChannel.class;
      bossGroup = new NioEventLoopGroup(1);
      workerGroup = new NioEventLoopGroup(workerNum);
      ((NioEventLoopGroup) workerGroup).setIoRatio(ioRatio);
    }

    LOG.info("[PS-THREAD-POOL] matrix-transport netty bossThreads=1"
        + ", workerThreads=" + workerNum
        + ", ioRatio=" + ioRatio
        + ", channelType=" + channelType
        + ", autoEpoll=" + autoEpoll
        + ", useEpoll=" + wantEpoll);

    LOG.info("Server port = " + port + ", backlog=" + backlog
        + ", tcpNoDelay=" + tcpNoDelay
        + ", writeBufferWatermark=[" + writeLow + "," + writeHigh + "]");

    final WriteBufferWaterMark watermark = new WriteBufferWaterMark(writeLow, writeHigh);
    final boolean tcpNoDelayFinal = tcpNoDelay;
    ServerBootstrap b = new ServerBootstrap();
    b.group(bossGroup, workerGroup).channel(channelClass)
      // Parent (listening) socket: only SO_BACKLOG matters here. SO_RCVBUF on the parent
      // is the initial RCVBUF inherited by accepted children, so we keep it on the parent
      // for compatibility with the historical configuration shape.
      .option(ChannelOption.SO_BACKLOG, backlog)
      .option(ChannelOption.SO_RCVBUF, recvBuffSize)
      // Child (accepted) socket options. SO_SNDBUF is meaningful on accepted children
      // (parent never writes), and SO_KEEPALIVE / TCP_NODELAY apply per-connection.
      .childOption(ChannelOption.SO_SNDBUF, sendBuffSize)
      .childOption(ChannelOption.SO_RCVBUF, recvBuffSize)
      .childOption(ChannelOption.SO_KEEPALIVE, true)
      .childOption(ChannelOption.TCP_NODELAY, tcpNoDelayFinal)
      .childOption(ChannelOption.WRITE_BUFFER_WATER_MARK, watermark)
      .childHandler(new ChannelInitializer<SocketChannel>() {
        @Override public void initChannel(SocketChannel ch) throws Exception {
          ChannelPipeline p = ch.pipeline();
          p.addLast(new LengthFieldBasedFrameDecoder(maxMessageSize, 0, 4, 0, 4));
          p.addLast(new LengthFieldPrepender(4));
          // ALL-IDLE detection: fires when neither read nor write happens within the window.
          // Disabled when channelIdleSeconds <= 0. Placed BEFORE the business handler so the
          // IdleStateEvent reaches it via userEventTriggered.
          if (channelIdleSeconds > 0) {
            p.addLast("idleStateHandler",
              new IdleStateHandler(0, 0, channelIdleSeconds, TimeUnit.SECONDS));
          }
          p.addLast(new MatrixTransportServerHandler(context));
        }
      });

    channelFuture = bindWithRetry(b);

    // Startup readiness gate: don't report PS startup as "done" until the matrix-transport
    // listen socket is actually bound. Without this, a brief race can let clients connect
    // before bind completes and they observe transient "connection refused".
    Channel serverChannel = channelFuture.channel();
    LOG.info("[PS-SERVER-READY][MATRIX-TRANSPORT] bound=true"
        + ", requestedPort=" + port
        + ", localAddress=" + serverChannel.localAddress()
        + ", active=" + serverChannel.isActive()
        + ", open=" + serverChannel.isOpen());
  }

  /**
   * Bind the data port, retrying a few times on "address already in use".
   *
   * <p>The data port is {@code control port + 1} by protocol (clients derive it from the PS
   * location, see {@code MatrixTransportClient}), so it cannot be swapped for another one:
   * all we can do is release the reservation as late as possible and retry the same port if
   * somebody transiently holds it.
   */
  private ChannelFuture bindWithRetry(ServerBootstrap b) {
    for (int i = 1; i <= MAX_BIND_TRY; i++) {
      // Release the reserved data port right before the real bind so that the window in
      // which another process could steal it stays as small as possible.
      releasePortReservation();

      ChannelFuture future = b.bind(port);
      if (!future.awaitUninterruptibly(30, TimeUnit.SECONDS)) {
        throw new IllegalStateException(
            "Matrix transport server bind timed out after 30000ms, requestedPort=" + port);
      }
      if (future.isSuccess()) {
        return future;
      }

      Throwable cause = future.cause();
      if (i == MAX_BIND_TRY || !NetUtils.isAddressInUse(cause)) {
        throw new IllegalStateException(
            "Matrix transport server bind failed, requestedPort=" + port, cause);
      }

      LOG.warn("Bind matrix transport server on port " + port + " failed (attempt " + i + "/"
          + MAX_BIND_TRY + "), retry in " + BIND_RETRY_INTERVAL_MS + "ms", cause);
      try {
        Thread.sleep(BIND_RETRY_INTERVAL_MS);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(
            "Interrupted while retrying matrix transport server bind, requestedPort=" + port, cause);
      }
    }

    // Unreachable: the loop either returns or throws.
    throw new IllegalStateException("Matrix transport server bind failed, requestedPort=" + port);
  }

  private void releasePortReservation() {
    NetUtils.PortReservation reservation = this.portReservation;
    if (reservation != null) {
      reservation.releaseDataPort();
    }
  }


  public void stop() throws InterruptedException {
    if (!stopped.getAndSet(true)) {
      try {
        if (channelFuture != null) {
          channelFuture.channel().close();
          channelFuture = null;
        }
      } finally {
        if (bossGroup != null) {
          bossGroup.shutdownGracefully();
          bossGroup = null;
        }

        if (workerGroup != null) {
          workerGroup.shutdownGracefully();
        }
      }
    }
  }
}