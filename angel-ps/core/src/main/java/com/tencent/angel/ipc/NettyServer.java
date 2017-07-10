/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ipc;

import com.google.protobuf.Message;
import com.tencent.angel.conf.TConstants;
import com.tencent.angel.exception.StandbyException;
import com.tencent.angel.ipc.NettyTransportCodec.NettyDataPack;
import com.tencent.angel.ipc.NettyTransportCodec.NettyFrameDecoder;
import com.tencent.angel.ipc.NettyTransportCodec.NettyFrameEncoder;
import com.tencent.angel.utils.ByteBufferInputStream;
import com.tencent.angel.utils.ByteBufferOutputStream;
import com.tencent.angel.utils.StringUtils;
import com.tencent.angel.protobuf.generated.RPCProtos.*;
import com.tencent.angel.protobuf.generated.RPCProtos.RpcResponseHeader.Status;
import io.netty.channel.*;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.socket.SocketChannel;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Netty-based RPC implementation.
 */
public abstract class NettyServer implements RpcServer {

  private static final Logger LOG = LoggerFactory.getLogger(NettyServer.class.getName());

  private ServerBootstrap bootstrap;
  private ChannelFuture channelFuture;
  private int port = -1;
  private AtomicInteger numConnections= new AtomicInteger();
  private final CountDownLatch closed = new CountDownLatch(1);

  private final static Map<Thread, String> thread2Address =
      new ConcurrentHashMap<Thread, String>();

  private Map<String, Class<? extends VersionedProtocol>> className2Class =
      new HashMap<String, Class<? extends VersionedProtocol>>();

  private volatile boolean started = false;

  final InetSocketAddress addr;

  public NettyServer(InetSocketAddress addr, Configuration conf) {
    int nThreads = conf.getInt(TConstants.SERVER_IO_THREAD, Runtime
        .getRuntime().availableProcessors() * 2);
    IOMode ioMode = IOMode.valueOf(conf.get(TConstants.NETWORK_IO_MODE, "NIO"));
    EventLoopGroup bossGroup =
        NettyUtils.createEventLoop(ioMode, nThreads,   "ML-server");
    EventLoopGroup workerGroup = bossGroup;
    PooledByteBufAllocator allocator =
        NettyUtils.createPooledByteBufAllocator(true, true, nThreads);
    bootstrap = new ServerBootstrap()
        .group(bossGroup, workerGroup)
        .channel(NettyUtils.getServerChannelClass(ioMode))
        .option(ChannelOption.ALLOCATOR, allocator)
        .childOption(ChannelOption.ALLOCATOR, allocator);
    bootstrap.option(ChannelOption.TCP_NODELAY, true);
    bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
    bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
    bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
    bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline()
            .addLast("encoder", NettyFrameEncoder.INSTANCE)
            .addLast("frameDecoder", NettyUtils.createFrameDecoder())
            .addLast("decoder", NettyFrameDecoder.INSTANCE)
            .addLast("handler", new NettyServerMLHandler());
      }
    });
    channelFuture = bootstrap.bind(addr);
    channelFuture.syncUninterruptibly();

    port = ((InetSocketAddress) channelFuture.channel().localAddress()).getPort();
    LOG.debug("server started on port: {}", port);
    this.addr = addr;
  }

  public static String getRemoteAddress() {
    return thread2Address.get(Thread.currentThread());
  }

  @Override
  public void stop() {
    LOG.info("Stopping server on " + getPort());
    if (channelFuture != null) {
      // close is a local operation and should finish within milliseconds; timeout just to be safe
      channelFuture.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS);
      channelFuture = null;
    }

    if (bootstrap != null && bootstrap.group() != null) {
      bootstrap.group().shutdownGracefully();
    }
    if (bootstrap != null && bootstrap.childGroup() != null) {
      bootstrap.childGroup().shutdownGracefully();
    }
    bootstrap = null;
    closed.countDown();
  }

  @Override
  public int getPort() {
    if (port == -1) {
      throw new IllegalStateException("Server not initialized");
    }
    return port;
  }

  @Override
  public void join() throws InterruptedException {
    closed.await();
  }

  @Override
  public void openServer() {
    started = true;
  }

  @Override
  public int getNumberOfConnections() {
    return numConnections.get();
  }

  /**
   * ML server handler for the ML transport
   */
  class NettyServerMLHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      numConnections.incrementAndGet();
      super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      numConnections.decrementAndGet();
      super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object obj) throws Exception {
      try {
        if (!(obj instanceof NettyDataPack)) {
          ctx.fireChannelRead(obj);
          return;
        }
        String errorClass = null;
        String error = null;
        Message returnValue = null;
        NettyDataPack dataPack = (NettyDataPack) obj;
        thread2Address.put(Thread.currentThread(), NettyUtils.getRemoteAddress(ctx.channel()));
        List<ByteBuffer> req = dataPack.getDatas();
        ByteBufferInputStream dis = new ByteBufferInputStream(req);
        ConnectionHeader header = ConnectionHeader.parseDelimitedFrom(dis);
        @SuppressWarnings("unused")
        RpcRequestHeader request = RpcRequestHeader.parseDelimitedFrom(dis);
        RpcRequestBody rpcRequestBody;
        try {
          if (!started) {
            throw new ServerNotRunningYetException("Server is not running yet");
          }
          rpcRequestBody = RpcRequestBody.parseDelimitedFrom(dis);
          if (LOG.isDebugEnabled()) {
            LOG.debug("message received in server, serial: " + dataPack.getSerial() + ", channel: "
                + ctx.channel() + ", methodName: " + rpcRequestBody.getMethodName());
          }
        } catch (Throwable t) {
          LOG.warn("Unable to read call parameters for client " +
              NettyUtils.getRemoteAddress(ctx.channel()), t);
          List<ByteBuffer> res =
              prepareResponse(null, Status.FATAL, t.getClass().getName(),
                  "IPC server unable to read call parameters:" + t.getMessage());
          if (res != null) {
            dataPack.setDatas(res);
            NettyDataPack.writeDataPack(ctx.channel(), dataPack);
          }
          return;
        }

        try {
          Class<? extends VersionedProtocol> class_ = className2Class.get(header.getProtocol());
          if (class_ == null) {
            try {
              className2Class.put(header.getProtocol(),
                  (Class<? extends VersionedProtocol>) Class.forName(header.getProtocol()));
            } catch (Exception e1) {
              LOG.error("ClassNotFoundException.", e1);
              throw new ClassNotFoundException(e1.getMessage(), e1);
            }
            class_ = className2Class.get(header.getProtocol());
          }

          returnValue = call(class_, rpcRequestBody, System.currentTimeMillis());
        } catch (Throwable e2) {
          LOG.error(Thread.currentThread().getName() + ", call " + rpcRequestBody + ": error: ", e2);
          if (e2.getCause() != null && e2.getCause() instanceof StandbyException) {
            errorClass = e2.getCause().getClass().getName();
            error = e2.getCause().getMessage();
          } else {
            errorClass = e2.getClass().getName();
            error = e2.getMessage();
          }
        }
        List<ByteBuffer> res =
            prepareResponse(returnValue, errorClass == null ? Status.SUCCESS : Status.ERROR,
                errorClass, error);

        // response will be null for one way messages.
        if (res != null) {
          dataPack.setDatas(res);
          NettyDataPack.writeDataPack(ctx.channel(), dataPack);
          if (LOG.isDebugEnabled()) {
            LOG.debug("response message in server, serial: " + dataPack.getSerial() + ", channel: "
                + ctx.channel() + ", for methodName: " + rpcRequestBody.getMethodName());
          }
        }
      } catch (OutOfMemoryError e4) {
        throw e4;
      } catch (ClosedChannelException cce) {
        LOG.warn(Thread.currentThread().getName() + " caught a ClosedChannelException, "
            + "this means that the server was processing a "
            + "request but the client went away. The error message was: " + cce.getMessage());
      } catch (Exception e4) {
        LOG.warn(Thread.currentThread().getName() + " caught: "
            + StringUtils.stringifyException(e4));
      }
    }

    protected List<ByteBuffer> prepareResponse(Object value, Status status, String errorClass,
        String error) {
      ByteBufferOutputStream buf = new ByteBufferOutputStream();
      DataOutputStream out = new DataOutputStream(buf);
      try {
        RpcResponseHeader.Builder builder = RpcResponseHeader.newBuilder();
        builder.setStatus(status);
        builder.build().writeDelimitedTo(out);
        if (error != null) {
          RpcException.Builder b = RpcException.newBuilder();
          b.setExceptionName(errorClass);
          b.setStackTrace(error);
          b.build().writeDelimitedTo(out);
        } else {
          if (value != null) {
            ((Message) value).writeDelimitedTo(out);
          }
        }
      } catch (IOException e) {
        LOG.warn("Exception while creating response " + e);
      }
      return buf.getBufferList();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.warn("Unexpected exception from downstream.Remote Host:"
          + NettyUtils.getRemoteAddress(ctx.channel()), cause);
      ctx.close();
    }
  }

  /**
   * @see com.tencent.angel.ipc.RpcServer#start()
   */
  @Override
  public void start() {}
}
