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
import org.apache.hadoop.conf.Configuration;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

/**
 * A Netty-based RPC implementation.
 */
public abstract class NettyServer implements RpcServer {

  private static final Logger LOG = LoggerFactory.getLogger(NettyServer.class.getName());

  public static final String NETTY_TCP_NODELAY_OPTION = "tcpNoDelay";
  public static final boolean DEFAULT_TCP_NODELAY_VALUE = true;
  public static final String NETTY_KEEP_ALIVE_OPTION = "keepAlive";
  public static final boolean NETTY_KEEP_ALIVE_VALUE = true;

  private final Channel serverChannel;
  private final ChannelGroup allChannels = new DefaultChannelGroup("ml-netty-server");
  private final ChannelFactory channelFactory;
  private final CountDownLatch closed = new CountDownLatch(1);

  private final static Map<Thread, InetSocketAddress> thread2Address =
      new ConcurrentHashMap<Thread, InetSocketAddress>();

  private Map<String, Class<? extends VersionedProtocol>> className2Class =
      new HashMap<String, Class<? extends VersionedProtocol>>();

  private volatile boolean started = false;

  final InetSocketAddress addr;

  public NettyServer(InetSocketAddress addr, Configuration conf) {
    this(addr, new NioServerSocketChannelFactory(Executors.newCachedThreadPool(),
        Executors.newCachedThreadPool(), conf.getInt(TConstants.SERVER_IO_THREAD, Runtime
            .getRuntime().availableProcessors() * 2)), conf);
  }

  public NettyServer(InetSocketAddress addr, ChannelFactory channelFactory, Configuration conf) {
    this.channelFactory = channelFactory;
    ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline p = Channels.pipeline();
        p.addLast("frameDecoder", new NettyFrameDecoder());
        p.addLast("frameEncoder", new NettyFrameEncoder());
        p.addLast("handler", new NettyServerMLHandler());
        return p;
      }
    });
    this.addr = addr;
    bootstrap.setOptions(buildDefaultBootstrapOptions());
    serverChannel = bootstrap.bind(addr);
    allChannels.add(serverChannel);
  }

  public static InetSocketAddress getRemoteAddress() {
    return thread2Address.get(Thread.currentThread());
  }

  private static Map<String, Object> buildDefaultBootstrapOptions() {
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(NETTY_TCP_NODELAY_OPTION, DEFAULT_TCP_NODELAY_VALUE);
    options.put("child." + NETTY_TCP_NODELAY_OPTION, DEFAULT_TCP_NODELAY_VALUE);
    options.put(NETTY_KEEP_ALIVE_OPTION, NETTY_KEEP_ALIVE_VALUE);
    options.put("child." + NETTY_KEEP_ALIVE_OPTION, NETTY_KEEP_ALIVE_VALUE);
    return options;
  }

  @Override
  public void stop() {
    LOG.info("Stopping server on " + this.addr.getPort());
    ChannelGroupFuture future = allChannels.close();
    future.awaitUninterruptibly();
    channelFactory.releaseExternalResources();
    serverChannel.close();
    closed.countDown();
  }

  @Override
  public int getPort() {
    return ((InetSocketAddress) serverChannel.getLocalAddress()).getPort();
  }

  public InetSocketAddress getLocalAddress() {
    return ((InetSocketAddress) serverChannel.getLocalAddress());
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
    return allChannels.size() - 1;
  }

  /**
   * ML server handler for the ML transport
   */
  class NettyServerMLHandler extends SimpleChannelUpstreamHandler {

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
      if (e instanceof ChannelStateEvent) {
        LOG.debug(e.toString());
      }
      super.handleUpstream(ctx, e);
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
      allChannels.add(e.getChannel());
      super.channelOpen(ctx, e);
    }

    public String getRemoteAddress() {
      return thread2Address.get(Thread.currentThread()).toString();
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
      try {
        String errorClass = null;
        String error = null;
        Message returnValue = null;
        thread2Address.put(Thread.currentThread(), (InetSocketAddress) ctx.getChannel()
            .getRemoteAddress());
        NettyDataPack dataPack = (NettyDataPack) e.getMessage();
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
                + ctx.getChannel() + ", methodName: " + rpcRequestBody.getMethodName());
          }
        } catch (Throwable t) {
          LOG.warn("Unable to read call parameters for client " + getRemoteAddress(), t);
          List<ByteBuffer> res =
              prepareResponse(null, Status.FATAL, t.getClass().getName(),
                  "IPC server unable to read call parameters:" + t.getMessage());
          if (res != null) {
            dataPack.setDatas(res);
            e.getChannel().write(dataPack);
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
          e.getChannel().write(dataPack);
          if (LOG.isDebugEnabled()) {
            LOG.debug("response message in server, serial: " + dataPack.getSerial() + ", channel: "
                + ctx.getChannel() + ", for methodName: " + rpcRequestBody.getMethodName());
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
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
      allChannels.remove(e.getChannel());
      super.channelClosed(ctx, e);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      LOG.warn("Unexpected exception from downstream.Remote Host:"
          + e.getChannel().getRemoteAddress(), e.getCause());
      e.getChannel().close();
    }
  }

  /**
   * @see com.tencent.angel.ipc.RpcServer#start()
   */
  @Override
  public void start() {}
}
