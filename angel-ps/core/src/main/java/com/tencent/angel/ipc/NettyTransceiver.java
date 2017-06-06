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
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.conf.TConstants;
import com.tencent.angel.exception.RemoteException;
import com.tencent.angel.exception.StandbyException;
import com.tencent.angel.ipc.NettyTransportCodec.NettyDataPack;
import com.tencent.angel.ipc.NettyTransportCodec.NettyFrameDecoder;
import com.tencent.angel.ipc.NettyTransportCodec.NettyFrameEncoder;
import com.tencent.angel.utils.ByteBufferInputStream;
import com.tencent.angel.utils.ByteBufferOutputStream;
import com.tencent.angel.protobuf.generated.RPCProtos;
import com.tencent.angel.protobuf.generated.RPCProtos.*;
import com.tencent.angel.protobuf.generated.RPCProtos.RpcResponseHeader.Status;
import org.apache.hadoop.conf.Configuration;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A Netty-based {@link Transceiver} implementation.
 */
public class NettyTransceiver extends Transceiver {
  public static final String NETTY_CONNECT_TIMEOUT_OPTION = "connectTimeoutMillis";
  public static final String NETTY_TCP_NODELAY_OPTION = "tcpNoDelay";
  public static final boolean DEFAULT_TCP_NODELAY_VALUE = true;
  public static final String NETTY_KEEP_ALIVE_OPTION = "keepAlive";
  public static final boolean NETTY_KEEP_ALIVE_VALUE = true;

  private static final Logger LOG = LoggerFactory.getLogger(NettyTransceiver.class.getName());

  private final AtomicInteger serialGenerator = new AtomicInteger(0);
  private final Map<Integer, Callback<List<ByteBuffer>>> requests =
      new ConcurrentHashMap<Integer, Callback<List<ByteBuffer>>>();

  private final ChannelFactory channelFactory;
  private final long connectTimeoutMillis;
  private final ClientBootstrap bootstrap;
  private final InetSocketAddress remoteAddr;

  volatile ChannelFuture channelFuture;
  volatile boolean stopping;
  private final Object channelFutureLock = new Object();

  private int refCount = 1;

  private Configuration conf = new Configuration();

  private Timer timer = new HashedWheelTimer();
  /**
   * Read lock must be acquired whenever using non-final state. Write lock must be acquired whenever
   * modifying state.
   */
  private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();
  private Channel channel; // Synchronized on stateLock

  NettyTransceiver() {
    channelFactory = null;
    connectTimeoutMillis = 0L;
    bootstrap = null;
    remoteAddr = null;
  }

  /**
   * Creates a NettyTransceiver, and attempts to connect to the given address.
   * {@link TConstants#DEFAULT_CONNECTION_TIMEOUT_MILLIS} is used for the connection timeout.
   * 
   * @param addr the address to connect to.
   * @throws java.io.IOException if an error occurs connecting to the given address.
   */
  public NettyTransceiver(InetSocketAddress addr) throws IOException {
    this(addr, TConstants.DEFAULT_CONNECTION_TIMEOUT_MILLIS);
  }

  /**
   * Creates a NettyTransceiver, and attempts to connect to the given address.
   * 
   * @param addr the address to connect to.
   * @param connectTimeoutMillis maximum amount of time to wait for connection establishment in
   *        milliseconds, or null to use {@link TConstants#DEFAULT_CONNECTION_TIMEOUT_MILLIS}.
   * @throws java.io.IOException if an error occurs connecting to the given address.
   */
  public NettyTransceiver(InetSocketAddress addr, Long connectTimeoutMillis) throws IOException {
    this(addr, new NioClientSocketChannelFactory(
        Executors.newCachedThreadPool(new NettyTransceiverThreadFactory("ML "
            + NettyTransceiver.class.getSimpleName() + " Boss")),
        Executors.newCachedThreadPool(new NettyTransceiverThreadFactory("ML "
            + NettyTransceiver.class.getSimpleName() + " I/O Worker")), 1), connectTimeoutMillis);
  }

  /**
   * Creates a NettyTransceiver, and attempts to connect to the given address.
   * {@link TConstants#DEFAULT_CONNECTION_TIMEOUT_MILLIS} is used for the connection timeout.
   * 
   * @param addr the address to connect to.
   * @param channelFactory the factory to use to create a new Netty Channel.
   * @throws java.io.IOException if an error occurs connecting to the given address.
   */
  public NettyTransceiver(InetSocketAddress addr, ChannelFactory channelFactory) throws IOException {
    this(addr, channelFactory, buildDefaultBootstrapOptions(null));
  }

  /**
   * Creates a NettyTransceiver, and attempts to connect to the given address.
   * 
   * @param addr the address to connect to.
   * @param channelFactory the factory to use to create a new Netty Channel.
   * @param connectTimeoutMillis maximum amount of time to wait for connection establishment in
   *        milliseconds, or null to use {@link TConstants#DEFAULT_CONNECTION_TIMEOUT_MILLIS}.
   * @throws java.io.IOException if an error occurs connecting to the given address.
   */
  public NettyTransceiver(InetSocketAddress addr, ChannelFactory channelFactory,
      Long connectTimeoutMillis) throws IOException {
    this(addr, channelFactory, buildDefaultBootstrapOptions(connectTimeoutMillis));
  }

  /**
   * Creates a NettyTransceiver, and attempts to connect to the given address. It is strongly
   * recommended that the {@link #NETTY_CONNECT_TIMEOUT_OPTION} option be set to a reasonable
   * timeout value (a Long value in milliseconds) to prevent connect/disconnect attempts from
   * hanging indefinitely. It is also recommended that the {@link #NETTY_TCP_NODELAY_OPTION} option
   * be set to true to minimize RPC latency.
   * 
   * @param addr the address to connect to.
   * @param channelFactory the factory to use to create a new Netty Channel.
   * @param nettyClientBootstrapOptions map of Netty ClientBootstrap options to use.
   * @throws java.io.IOException if an error occurs connecting to the given address.
   */
  public NettyTransceiver(InetSocketAddress addr, ChannelFactory channelFactory,
      Map<String, Object> nettyClientBootstrapOptions) throws IOException {
    // DefaultChannelFuture.setUseDeadLockChecker(false);
    if (channelFactory == null) {
      throw new NullPointerException("channelFactory is null");
    }

    // Set up.
    this.channelFactory = channelFactory;
    this.connectTimeoutMillis =
        (Long) nettyClientBootstrapOptions.get(NETTY_CONNECT_TIMEOUT_OPTION);
    bootstrap = new ClientBootstrap(channelFactory);
    remoteAddr = addr;

    // Configure the event pipeline factory.
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline p = Channels.pipeline();
        p.addLast("frameDecoder", new NettyFrameDecoder());
        p.addLast("frameEncoder", new NettyFrameEncoder());

        p.addLast(
            "readTimeout",
            new ReadTimeoutHandler(timer, NettyTransceiver.this.conf.getInt(
                TConstants.CONNECTION_READ_TIMEOUT_SEC,
                TConstants.DEFAULT_CONNECTION_READ_TIMEOUT_SEC)));

        p.addLast("handler", new MLClientMLHandler());
        return p;
      }
    });

    if (nettyClientBootstrapOptions != null) {
      LOG.debug("Using Netty bootstrap options: " + nettyClientBootstrapOptions);
      bootstrap.setOptions(nettyClientBootstrapOptions);
    }

    // Make a new connection.
    stateLock.readLock().lock();
    try {
      getChannel();
    } catch (IOException e) {
      LOG.debug("connect error, e: " + e);
      throw e;
    } finally {
      stateLock.readLock().unlock();
    }
  }

  /**
   * Creates the default options map for the Netty ClientBootstrap.
   * 
   * @param connectTimeoutMillis connection timeout in milliseconds, or null if no timeout is
   *        desired.
   * @return the map of Netty bootstrap options.
   */
  private static Map<String, Object> buildDefaultBootstrapOptions(Long connectTimeoutMillis) {
    Map<String, Object> options = new HashMap<String, Object>(2);
    options.put(NETTY_TCP_NODELAY_OPTION, DEFAULT_TCP_NODELAY_VALUE);
    options.put(NETTY_CONNECT_TIMEOUT_OPTION,
        connectTimeoutMillis == null ? TConstants.DEFAULT_CONNECTION_TIMEOUT_MILLIS
            : connectTimeoutMillis);
    options.put(NETTY_KEEP_ALIVE_OPTION, NETTY_KEEP_ALIVE_VALUE);
    options.put("child." + NETTY_KEEP_ALIVE_OPTION, NETTY_KEEP_ALIVE_VALUE);
    return options;
  }

  /**
   * Tests whether the given channel is ready for writing.
   * 
   * @return true if the channel is open and ready; false otherwise.
   */
  private static boolean isChannelReady(Channel channel) {
    return (channel != null) && channel.isOpen() && channel.isBound() && channel.isConnected();
  }

  /**
   * Gets the Netty channel. If the channel is not connected, first attempts to connect. NOTE: The
   * stateLock read lock *must* be acquired before calling this method.
   * 
   * @return the Netty channel
   * @throws java.io.IOException if an error occurs connecting the channel.
   */
  private Channel getChannel() throws IOException {
    if (!isChannelReady(channel)) {
      // Need to reconnect
      // Upgrade to write lock
      stateLock.readLock().unlock();
      stateLock.writeLock().lock();
      try {
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
                channelFuture.cancel();
                throw new IOException("Error connecting to " + remoteAddr, channelFuture.getCause());
              }
              channel = channelFuture.getChannel();
              if (LOG.isDebugEnabled()) {
                LOG.debug("new channel is {} ", channel);
              }
              channelFuture = null;
            }
          }
        }
      } finally {
        // Downgrade to read lock:
        stateLock.readLock().lock();
        stateLock.writeLock().unlock();
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
  private void disconnect(Channel channel, boolean awaitCompletion, boolean cancelPendingRequests,
      Throwable cause) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("disconnecting channel: " + channel);
    }
    Channel channelToClose = null;
    Map<Integer, Callback<List<ByteBuffer>>> requestsToCancel = null;
    boolean stateReadLockHeld = stateLock.getReadHoldCount() != 0;

    ChannelFuture channelFutureToCancel = null;
    synchronized (channelFutureLock) {
      if (stopping && channelFuture != null) {
        channelFutureToCancel = channelFuture;
        channelFuture = null;
      }
    }
    if (channelFutureToCancel != null) {
      channelFutureToCancel.cancel();
    }

    if (stateReadLockHeld) {
      stateLock.readLock().unlock();
    }

    // (TODO: why use writeLock? why not use this.channel instead of channel?
    stateLock.writeLock().lock();
    try {
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
        channel = null;

        if (cancelPendingRequests) {
          // Remove all pending requests (will be canceled after relinquishing
          // write lock).
          requestsToCancel = new ConcurrentHashMap<Integer, Callback<List<ByteBuffer>>>(requests);
          requests.clear();
        }
      }
    } finally {
      if (stateReadLockHeld) {
        stateLock.readLock().lock();
      }
      stateLock.writeLock().unlock();
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
      timer.stop();
    } finally {
      // Shut down all thread pools to exit.
      if (LOG.isDebugEnabled()) {
        LOG.debug("release channelFactory resource for " + remoteAddr);
      }
      channelFactory.releaseExternalResources();
      bootstrap.shutdown();
    }
  }

  @Override
  public String getRemoteName() throws IOException {
    stateLock.readLock().lock();
    try {
      return getChannel().getRemoteAddress().toString();
    } finally {
      stateLock.readLock().unlock();
    }
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
        return future.get(conf.getLong(AngelConfiguration.ANGEL_READ_TIMEOUT_SEC,
            AngelConfiguration.DEFAULT_ANGEL_READ_TIMEOUT_SEC), TimeUnit.SECONDS);
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
    stateLock.readLock().lock();
    int serial = serialGenerator.incrementAndGet();
    try {
      NettyDataPack dataPack = new NettyDataPack(serial, request);
      requests.put(serial, callback);
      if (LOG.isDebugEnabled()) {
        LOG.debug("send message, serial: " + serial + ", channel: " + channel);
      }
      // LOG.info("serial " + serial + "start time = " + System.currentTimeMillis());
      writeDataPack(dataPack);
    } catch (IOException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("write Data error, serial: " + serial + ", channel: " + channel + " due to:", e);
      }
      requests.remove(serial);
      callback.handleError(e);
    } finally {
      stateLock.readLock().unlock();
    }
  }

  @Override
  public void writeBuffers(List<ByteBuffer> buffers) throws IOException {
    stateLock.readLock().lock();
    try {
      writeDataPack(new NettyDataPack(serialGenerator.incrementAndGet(), buffers));
    } finally {
      stateLock.readLock().unlock();
    }
  }

  /**
   * Writes a NettyDataPack, reconnecting to the remote peer if necessary. NOTE: The stateLock read
   * lock *must* be acquired before calling this method.
   * 
   * @param dataPack the data pack to write.
   * @throws java.io.IOException if an error occurs connecting to the remote peer.
   */
  private void writeDataPack(NettyDataPack dataPack) throws IOException {
    getChannel().write(dataPack);
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
  class MLClientMLHandler extends SimpleChannelUpstreamHandler {


    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
      super.handleUpstream(ctx, e);
    }


    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
      // channel = e.getChannel();
      super.channelOpen(ctx, e);
    }

    @Override
    public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Remote peer " + remoteAddr + " closed channel: " + e.getChannel());
        disconnect(e.getChannel(), false, true, null);
      }
      // channel = e.getChannel();
      super.channelClosed(ctx, e);
    }


    @Override
    public void messageReceived(ChannelHandlerContext ctx, final MessageEvent e) {
      NettyDataPack dataPack = (NettyDataPack) e.getMessage();
      if (LOG.isDebugEnabled()) {
        LOG.debug("messageReceived, serail: " + dataPack.getSerial() + ", channel: "
            + ctx.getChannel());
      }

      // LOG.info("method " + dataPack.getSerial() + " received ts = " +
      // System.currentTimeMillis());

      Callback<List<ByteBuffer>> callback = requests.get(dataPack.getSerial());
      if (callback == null) {
        LOG.error("Missing previous call info, serail: " + dataPack.getSerial() + ", channel: "
            + ctx.getChannel());
        throw new RuntimeException("Missing previous call info");
      }
      try {
        callback.handleResult(dataPack.getDatas());
      } finally {
        requests.remove(dataPack.getSerial());
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Netty Transceiver error." + e.toString() + ", channel: " + ctx.getChannel(),
            e.getCause());
      }
      disconnect(e.getChannel(), false, true, e.getCause());
    }
  }

  /**
   * Creates threads with unique names based on a specified name prefix.
   */
  private static class NettyTransceiverThreadFactory implements ThreadFactory {
    private final AtomicInteger threadId = new AtomicInteger(0);
    private final String prefix;

    /**
     * Creates a NettyTransceiverThreadFactory that creates threads with the specified name.
     * 
     * @param prefix the name prefix to use for all threads created by this ThreadFactory. A unique
     *        ID will be appended to this prefix to form the final thread name.
     */
    public NettyTransceiverThreadFactory(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = new Thread(r);
      thread.setName(prefix + " " + threadId.incrementAndGet());
      return thread;
    }
  }

  /**
   * Increment this client's reference count
   * 
   */
  synchronized void incCount() {
    refCount++;
  }

  /**
   * Decrement this client's reference count
   * 
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

  /**
   */
  @Override
  public Configuration getConf() {
    return this.conf;
  }

  /**
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
}
