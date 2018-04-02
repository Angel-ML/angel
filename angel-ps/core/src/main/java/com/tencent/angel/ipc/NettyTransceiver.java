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

/**
 * Modify close method to fix Angel client exit problem.
 */
package com.tencent.angel.ipc;

import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.RemoteException;
import com.tencent.angel.exception.StandbyException;
import com.tencent.angel.ipc.NettyTransportCodec.NettyDataPack;
import com.tencent.angel.ipc.NettyTransportCodec.NettyFrameDecoder;
import com.tencent.angel.ipc.NettyTransportCodec.NettyFrameEncoder;
import com.tencent.angel.protobuf.generated.RPCProtos;
import com.tencent.angel.protobuf.generated.RPCProtos.*;
import com.tencent.angel.protobuf.generated.RPCProtos.RpcResponseHeader.Status;
import com.tencent.angel.utils.ByteBufferInputStream;
import com.tencent.angel.utils.ByteBufferOutputStream;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.ReadTimeoutHandler;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Netty-based {@link Transceiver} implementation.
 */
public class NettyTransceiver extends Transceiver {
  private static final Logger LOG = LoggerFactory.getLogger(NettyTransceiver.class.getName());

  private final AtomicInteger serialGenerator = new AtomicInteger(0);
  private final Map<Integer, Callback<List<ByteBuffer>>> requests =
          new ConcurrentHashMap<Integer, Callback<List<ByteBuffer>>>();

  private final int connectTimeoutMillis;
  private final Bootstrap bootstrap;
  private final InetSocketAddress remoteAddr;

  private volatile ChannelFuture channelFuture;
  private volatile boolean stopping;
  private volatile Channel channel; // Synchronized on stateLock
  private final Object channelFutureLock = new Object();

  private volatile int refCount = 1;
  private Configuration conf;

  public NettyTransceiver(
          Configuration conf,
          InetSocketAddress addr,
          EventLoopGroup workerGroup,
          PooledByteBufAllocator pooledAllocator,
          Class<? extends Channel> socketChannelClass,
          int connectTimeoutMillis) throws IOException {
    this.conf = conf;
    this.connectTimeoutMillis = connectTimeoutMillis;

    bootstrap = new Bootstrap();
    bootstrap
            .group(workerGroup)
            .channel(socketChannelClass)
            // Disable Nagle's Algorithm since we don't want packets to wait
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis)
            .option(ChannelOption.ALLOCATOR, pooledAllocator);

    // Configure the event pipeline factory.
    bootstrap.handler(new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        ch.pipeline()
                .addLast("encoder", NettyFrameEncoder.INSTANCE)
                .addLast("frameDecoder", NettyUtils.createFrameDecoder())
                .addLast("decoder", NettyFrameDecoder.INSTANCE)
                .addLast(
                        "readTimeout",
                        new ReadTimeoutHandler(NettyTransceiver.this.conf.getInt(
                                AngelConf.CONNECTION_READ_TIMEOUT_SEC,
                                AngelConf.DEFAULT_CONNECTION_READ_TIMEOUT_SEC)))
                .addLast("handler", new MLClientMLHandler());
      }
    });
    remoteAddr = addr;
    // Make a new connection.
    try {
      getChannel();
    } catch (IOException e) {
      LOG.debug("connect error, e: " + e);
      throw e;
    }
  }

  /**
   * Tests whether the given channel is ready for writing.
   *
   * @return true if the channel is open and ready; false otherwise.
   */
  private static boolean isChannelReady(Channel channel) {
    return (channel != null) && channel.isOpen() && channel.isRegistered() && channel.isActive();
  }

  /**
   * Gets the Netty channel. If the channel is not connected, first attempts to connect. NOTE: The
   * stateLock read lock *must* be acquired before calling this method.
   *
   * @return the Netty channel
   * @throws java.io.IOException if an error occurs connecting the channel.
   */
  private synchronized Channel getChannel() throws IOException {
    if (!isChannelReady(channel)) {
      synchronized (channelFutureLock) {
        if (!stopping) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Connecting to " + remoteAddr);
          }
          channelFuture = bootstrap.connect(remoteAddr);
        }
      }
      if (channelFuture != null) {
        try {
          channelFuture.await(connectTimeoutMillis);
          LOG.debug("waiting connect timeout! connectTimeoutMillis: " + connectTimeoutMillis);
        } catch (InterruptedException e) {
          stopping = false;
          throw new IOException("Request has been interrupted.", e);
        }

        synchronized (channelFutureLock) {
          if (!channelFuture.isSuccess()) {
            channelFuture.cancel(true);
            throw new IOException("Error connecting to " + remoteAddr, channelFuture.cause());
          }
          channel = channelFuture.channel();
          if (LOG.isDebugEnabled()) {
            LOG.debug("new channel is {} ", channel);
          }
          channelFuture = null;
        }
      }
    }
    return channel;
  }

  /**
   * Closes the connection to the remote peer if connected.
   *
   * @param awaitCompletion if true, will block until the close has completed.
   * @param cancelPendingRequests if true, will drain the requests map and send an IOException to
   *        all Callbacks.
   * @param cause if non-null and cancelPendingRequests is true, this Throwable will be passed to
   *        all Callbacks.
   */
  private synchronized void disconnect(
          Channel channel,
          boolean awaitCompletion,
          boolean cancelPendingRequests,
          Throwable cause) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("disconnecting channel: " + channel);
    }
    Channel channelToClose = null;
    Map<Integer, Callback<List<ByteBuffer>>> requestsToCancel = null;

    ChannelFuture channelFutureToCancel = null;
    synchronized (channelFutureLock) {
      if (stopping && channelFuture != null) {
        channelFutureToCancel = channelFuture;
        channelFuture = null;
      }
    }
    if (channelFutureToCancel != null) {
      channelFutureToCancel.cancel(true);
    }
    if (channel != null) {
      if (cause != null) {
        LOG.debug("Disconnect {} due to {}", channel,
                cause.getClass().getName() + cause.getMessage());
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Disconnect {}", this.channel);
        }
      }
      channelToClose = channel;
      this.channel = null;

      if (cancelPendingRequests) {
        // Remove all pending requests (will be canceled after relinquishing
        // write lock).
        requestsToCancel = new ConcurrentHashMap<Integer, Callback<List<ByteBuffer>>>(requests);
        requests.clear();
      }
    }

    // Cancel any pending requests by sending errors to the callbacks:
    if ((requestsToCancel != null) && !requestsToCancel.isEmpty()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Removing " + requestsToCancel.size() + " pending request(s).");
      }
      for (Callback<List<ByteBuffer>> request : requestsToCancel.values()) {
        request.handleError(cause != null ? cause : new IOException(getClass().getSimpleName()
                + " closed"));
      }
    }

    // Close the channel:
    if (channelToClose != null) {
      ChannelFuture closeFuture = channelToClose.close();
      if (awaitCompletion && (closeFuture != null)) {
        closeFuture.awaitUninterruptibly(connectTimeoutMillis);
      }
    }
  }

  /**
   * Netty channels are thread-safe, so there is no need to acquire locks. This method is a no-op.
   */
  @Override
  public void lockChannel() {

  }

  /**
   * Netty channels are thread-safe, so there is no need to acquire locks. This method is a no-op.
   */
  @Override
  public void unlockChannel() {

  }

  public synchronized void close() {
    if (stopping) {
      return;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Closing the netty transceiver...");
    }
    try {
      // Close the connection:
      stopping = true;
      disconnect(this.channel, true, true, null);
    } finally {
      // Shut down all thread pools to exit.
      if (LOG.isDebugEnabled()) {
        LOG.debug("release channelFactory resource for " + remoteAddr);
      }
    }
  }

  @Override
  public String getRemoteName() throws IOException {
    return NettyUtils.getRemoteAddress(getChannel());
  }

  /**
   * Make a call, passing <code>param</code>, to the IPC server running at <code>address</code>
   * which is servicing the <code>protocol</code> protocol, with the <code>ticket</code>
   * credentials, returning the value. Throws exceptions if there are network problems or if the
   * remote code threw an exception.
   */
  public Message call(RpcRequestBody requestBody, Class<? extends VersionedProtocol> protocol,
                      int rpcTimeout, Callback<Message> callback) throws Exception {
    ConnectionHeader.Builder builder = ConnectionHeader.newBuilder();
    builder.setProtocol(protocol == null ? "" : protocol.getName());
    ConnectionHeader connectionHeader = builder.build();

    RpcRequestHeader.Builder headerBuilder = RPCProtos.RpcRequestHeader.newBuilder();

    RpcRequestHeader rpcHeader = headerBuilder.build();

    ByteBufferOutputStream bbo = new ByteBufferOutputStream();
    connectionHeader.writeDelimitedTo(bbo);
    rpcHeader.writeDelimitedTo(bbo);
    requestBody.writeDelimitedTo(bbo);
    CallFuture<Message> future = new CallFuture<Message>(callback);
    if (LOG.isDebugEnabled()) {
      LOG.debug("send message, " + requestBody.getMethodName() + " , channel: " + channel);
    }

    transceive(bbo.getBufferList(), new TransceiverCallback<Message>(requestBody, protocol, future));

    if (callback == null) {
      try {
        return future.get(conf.getLong(AngelConf.ANGEL_READ_TIMEOUT_SEC,
                AngelConf.DEFAULT_ANGEL_READ_TIMEOUT_SEC), TimeUnit.SECONDS);
      } catch (java.util.concurrent.TimeoutException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("timeout for: send message, " + requestBody.getMethodName() + " , channel: "
                  + channel);
        }
        disconnect(this.channel, true, true, e);
        throw e;
      }
    }
    return null;
  }

  /**
   * Override as non-synchronized method because the method is thread safe.
   */
  @Override
  public List<ByteBuffer> transceive(List<ByteBuffer> request) throws IOException {
    try {
      CallFuture<List<ByteBuffer>> transceiverFuture = new CallFuture<List<ByteBuffer>>();
      transceive(request, transceiverFuture);
      return transceiverFuture.get();
    } catch (InterruptedException e) {
      LOG.info("failed to get the response", e);
      throw new IOException(e);
    } catch (ExecutionException e) {
      LOG.warn("failed to get the response", e);
      throw new IOException(e);
    }
  }

  @Override
  public void transceive(List<ByteBuffer> request, Callback<List<ByteBuffer>> callback) {
    int serial = serialGenerator.incrementAndGet();
    try {
      NettyDataPack dataPack = new NettyDataPack(serial, request);
      requests.put(serial, callback);
      if (LOG.isDebugEnabled()) {
        LOG.debug("send message, serial: " + serial + ", channel: " + channel);
      }
      // LOG.info("serial " + serial + "start time = " + System.currentTimeMillis());
      NettyDataPack.writeDataPack(getChannel(), dataPack);
    } catch (IOException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("write Data error, serial: " + serial + ", channel: " + channel + " due to:", e);
      }
      requests.remove(serial);
      callback.handleError(e);
    }
  }

  @Override
  public void writeBuffers(List<ByteBuffer> buffers) throws IOException {
    NettyDataPack dataPack = new NettyDataPack(serialGenerator.incrementAndGet(), buffers);
    NettyDataPack.writeDataPack(getChannel(), dataPack);
  }

  @Override
  public List<ByteBuffer> readBuffers() throws IOException {
    throw new UnsupportedOperationException();
  }

  class TransceiverCallback<T> implements Callback<List<ByteBuffer>> {
    private final RpcRequestBody requestBody;
    private final Class<? extends VersionedProtocol> protocol;
    private final Callback<T> callback;

    /**
     * Creates a TransceiverCallback.
     *
     * @param callback the callback to set.
     */
    public TransceiverCallback(RpcRequestBody requestBody,
                               Class<? extends VersionedProtocol> protocol, Callback<T> callback) {
      this.requestBody = requestBody;
      this.protocol = protocol;
      this.callback = callback;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void handleResult(List<ByteBuffer> responseBytes) {
      ByteBufferInputStream in = new ByteBufferInputStream(responseBytes);
      try {
        // See NettyServer.prepareResponse for where we write out the response.
        // It writes the call.id (int), a boolean signifying any error (and if
        // so the exception name/trace), and the response bytes

        // Read the call id.
        RpcResponseHeader response = RpcResponseHeader.parseDelimitedFrom(in);
        if (response == null) {
          LOG.error("response is null");
          // When the stream is closed, protobuf doesn't raise an EOFException,
          // instead, it returns a null message object.
          throw new EOFException();
        }

        Status status = response.getStatus();
        if (status == Status.SUCCESS) {
          Message rpcResponseType;
          try {
            rpcResponseType =
                    ProtobufRpcEngine.Invoker.getReturnProtoType(ProtobufRpcEngine.Server.getMethod(
                            protocol, requestBody.getMethodName()));
          } catch (Exception e) {
            throw new RuntimeException(e); // local exception
          }
          Builder builder = rpcResponseType.newBuilderForType();
          builder.mergeDelimitedFrom(in);
          Message value = builder.build();

          if (callback != null) {
            LOG.debug("to execute callback, method: " + requestBody.getMethodName());
            callback.handleResult((T) value);
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("callback is null, method: " + requestBody.getMethodName());
            }
          }
        } else if (status == Status.ERROR || status == Status.FATAL) {
          RpcException exceptionResponse = RpcException.parseDelimitedFrom(in);
          String exceptionName = exceptionResponse.getExceptionName();
          String exceptionReason = exceptionResponse.getStackTrace();
          if (exceptionName != null && exceptionName.equals(StandbyException.class.getName())) {
            handleError(new StandbyException(exceptionReason));
          } else {
            RemoteException remoteException = new RemoteException(exceptionName, exceptionReason);
            handleError(remoteException.unwrapRemoteException());
          }
        } else {
          handleError(new IOException("response status is " + status));
        }
      } catch (Exception e) {
        LOG.error("Error handling transceiver callback: " + e, e);
        handleError(e);
      }
    }

    @Override
    public void handleError(Throwable error) {
      callback.handleError(error);
    }
  }

  /**
   * ML client handler for the Netty transport
   */
  class MLClientMLHandler extends ChannelInboundHandlerAdapter {

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Remote peer " + remoteAddr + " closed channel: " + ctx.channel());
      }
      disconnect(ctx.channel(), false, true, null);
      super.channelInactive(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object request) throws Exception {
      if (!(request instanceof NettyDataPack)) {
        ctx.fireChannelRead(request);
        return;
      }
      NettyDataPack dataPack = (NettyDataPack) request;
      if (LOG.isDebugEnabled()) {
        LOG.debug("messageReceived, serail: " + dataPack.getSerial() + ", channel: "
                + ctx.channel());
      }

      // LOG.info("method " + dataPack.getSerial() + " received ts = " +
      // System.currentTimeMillis());

      Callback<List<ByteBuffer>> callback = requests.get(dataPack.getSerial());
      if (callback == null) {
        LOG.error("Missing previous call info, serail: " + dataPack.getSerial() + ", channel: "
                + ctx.channel());
        throw new RuntimeException("Missing previous call info");
      }
      try {
        callback.handleResult(dataPack.getDatas());
      } finally {
        requests.remove(dataPack.getSerial());
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Netty Transceiver error." + "channel: " + ctx.channel(), cause);
      }
      disconnect(ctx.channel(), false, true, cause);
    }
  }

  /**
   * Increment this client's reference count
   */
  synchronized void incCount() {
    refCount++;
  }

  /**
   * Decrement this client's reference count
   */
  synchronized void decCount() {
    refCount--;
  }

  /**
   * Return if this client has no reference
   *
   * @return true if this client has no reference; false otherwise
   */
  synchronized boolean isZeroReference() {
    return refCount == 0;
  }

  /**
   * @return the remoteAddr
   */
  public InetSocketAddress getRemoteAddr() {
    return remoteAddr;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
}