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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ps.client;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.common.transport.ChannelManager;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.matrix.transport.*;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.psagent.matrix.transport.FutureResult;
import com.tencent.angel.utils.ByteBufUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.conf.Configuration;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * PS RPC client
 */
public class PSClient {
  private static final Log LOG = LogFactory.getLog(PSClient.class);
  /** netty client bootstrap */
  private Bootstrap bootstrap;

  /** netty client thread pool */
  private EventLoopGroup eventGroup;

  /** channel pool manager:it maintain a channel pool for every server */
  private ChannelManager channelManager;

  /**
   * PS context
   */
  private final PSContext context;

  private final AtomicBoolean stopped;

  /**
   * Seq id generator
   */
  private final AtomicInteger seqIdGen;

  /**
   * Request seq id to rpc result map
   */
  private final ConcurrentHashMap<Integer, FutureResult> seqIdToResultMap;

  /**
   * Request seq id to Request map
   */
  private final ConcurrentHashMap<Integer, Request> seqIdToRequestMap;

  /**
   * Response message queue
   */
  private final LinkedBlockingQueue<ByteBuf> responseQueue;

  /**
   * Is use direct buffer for netty
   */
  private final boolean useDirectBuf;

  /**
   * Create a ps client
   * @param context PS context
   */
  public PSClient(PSContext context) {
    this.context = context;
    stopped = new AtomicBoolean(false);
    seqIdGen = new AtomicInteger(0);
    seqIdToResultMap = new ConcurrentHashMap<>();
    seqIdToRequestMap = new ConcurrentHashMap<>();
    responseQueue = new LinkedBlockingQueue<>();
    useDirectBuf = context.getConf().getBoolean(
      AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEDIRECTBUFFER,
      AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEDIRECTBUFFER);
  }

  /**
   * Init
   */
  public void init() {
    bootstrap = new Bootstrap();

    Configuration conf = context.getConf();
    int workerNum = conf.getInt(AngelConf.ANGEL_PS_HA_SYNC_WORKER_NUM,
      AngelConf.DEFAULT_ANGEL_PS_HA_SYNC_WORKER_NUM);

    channelManager = new ChannelManager(bootstrap, workerNum);

    int sendBuffSize = conf.getInt(AngelConf.ANGEL_PS_HA_SYNC_SEND_BUFFER_SIZE,
      AngelConf.DEFAULT_ANGEL_PS_HA_SYNC_SEND_BUFFER_SIZE);

    final int maxMessageSize =
      conf.getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_MAX_MESSAGE_SIZE,
        AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_MAX_MESSAGE_SIZE);

    //TODO: use Epoll for linux future
    /*Class channelClass = null;
    String os = System.getProperty("os.name");
    if(os.toLowerCase().startsWith("win")) {
      LOG.info("os is windows, we use NioEventLoopGroup");
      channelClass = NioSocketChannel.class;
      eventGroup = new NioEventLoopGroup(nettyWorkerNum);
      ((NioEventLoopGroup)eventGroup).setIoRatio(70);
    } else if(os.toLowerCase().startsWith("linux")) {

    }
    */

    eventGroup = new NioEventLoopGroup(workerNum);
    ((NioEventLoopGroup)eventGroup).setIoRatio(70);

    bootstrap.group(eventGroup).channel(NioSocketChannel.class)
      .option(ChannelOption.SO_SNDBUF, sendBuffSize)
      .handler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
          ChannelPipeline pipeLine = ch.pipeline();
          pipeLine.addLast(new LengthFieldBasedFrameDecoder(maxMessageSize, 0, 4, 0, 4));
          pipeLine.addLast(new LengthFieldPrepender(4));
          pipeLine.addLast(new PSClientHandler());
        }
      });
  }

  /**
   * Start
   */
  public void start() {
  }

  /**
   * Stop all services and worker threads.
   */
  public void stop() {
    if (!stopped.getAndSet(true)) {
      if (channelManager != null) {
        channelManager.clear();
      }
      eventGroup.shutdownGracefully();
    }
  }

  /**
   * Response Handler
   */
  class PSClientHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelActive(ChannelHandlerContext ctx) {}

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      LOG.debug("channel " + ctx.channel() + " inactive");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      LOG.debug("receive a message " + ((ByteBuf) msg).readableBytes());
      handleResponse((ByteBuf) msg);
    }
  }

  /**
   * Handle response message
   * @param msg response message
   */
  private void handleResponse(ByteBuf msg) {
    try {
      int seqId = msg.readInt();

      // find the partition request context from cache
      Request request = seqIdToRequestMap.remove(seqId);
      if (request == null) {
        return;
      }
      returnChannel(request);

      FutureResult<Response> result = seqIdToResultMap.remove(seqId);
      if(result == null) {
        return;
      }

      long startTs = System.currentTimeMillis();
      Response response = new Response();
      response.deserialize(msg);
      result.set(response);

      msg.release();

      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "handle response of request " + request + " use time=" + (System.currentTimeMillis()
            - startTs));
      }
    } catch (Throwable x) {
      LOG.fatal("hanlder rpc response failed ", x);
      context.getPs().failed("hanlder rpc response failed " + x.getMessage());
    }
  }

  /**
   * Send request
   * @param serverId dest ps id
   * @param location dest ps location
   * @param seqId request seq id
   * @param request request
   * @param msg serialized request
   * @param result request future result
   */
  private void send(ParameterServerId serverId, Location location, int seqId,
    Request request, ByteBuf msg, FutureResult<Response> result) {
    if (location == null) {
      String log = "server " + serverId + " location is null";
      LOG.error(log);
      result.set(new Response(ResponseType.SERVER_NOT_READY, log));
      return;
    }

    long startTs = System.currentTimeMillis();

    // get a channel to server from pool
    Channel channel;
    GenericObjectPool<Channel> pool;

    try {
      pool = getChannelPool(location);
      channel = pool.borrowObject();

      // if channel is not valid, it means maybe the connections to the server are closed
      if (!channel.isActive() || !channel.isOpen()) {
        String log = "channel " + channel + " is not active";
        LOG.error(log);
        // channelManager.removeChannelPool(loc);
        result.set(new Response(ResponseType.NETWORK_ERROR, log));
      }

      request.getContext().setChannelPool(pool);
      request.getContext().setChannel(channel);
      ChannelFuture cf = channel.writeAndFlush(msg);
      cf.addListener(new RequesterChannelFutureListener(seqId, request));
    } catch (Throwable x) {
      if(!stopped.get()) {
        LOG.error("get channel failed ", x);
      }
      String log = "get channel failed " + x.getMessage();
      result.set(new Response(ResponseType.NETWORK_ERROR, log));
    }
  }

  /**
   * Recover a matrix partition for a ps
   * @param serverId dest ps id
   * @param location dest ps location
   * @param part need recover partition
   * @return recover result
   */
  public FutureResult<Response> recoverPart(ParameterServerId serverId, Location location, ServerPartition part) {
    // Generate seq id
    int seqId = seqIdGen.incrementAndGet();
    FutureResult<Response> result = new FutureResult<>();
    seqIdToResultMap.put(seqId, result);

    // Create a RecoverPartRequest
    PartitionKey partKey = part.getPartitionKey();
    RecoverPartRequest request = new RecoverPartRequest(
      context.getClockVectorManager().getClockVec(partKey.getMatrixId(), partKey.getPartitionId()),
      new PartitionKey(partKey.getMatrixId(), partKey.getPartitionId()), part);
    request.getContext().setServerId(serverId);
    seqIdToRequestMap.put(seqId, request);

    // Serialize the request
    ByteBuf msg = ByteBufUtils.newByteBuf(16 + request.bufferLen(), useDirectBuf);
    msg.writeInt(-1);
    msg.writeInt(0);
    msg.writeInt(seqId);
    msg.writeInt(request.getType().getMethodId());
    request.serialize(msg);

    send(serverId, location, seqId, request, msg, result);
    return result;
  }

  /**
   * Put update data to another ps
   * @param serverId dest ps id
   * @param location dest ps location
   * @param request update request
   * @param update serialized update request
   * @return update result
   */
  public FutureResult<Response> put(ParameterServerId serverId, Location location,  Request request, ByteBuf update) {
    // Change the seqId for the request
    int seqId = seqIdGen.incrementAndGet();
    changeSeqId(seqId, update);

    request.getContext().setServerId(serverId);
    FutureResult<Response> result = new FutureResult<>();
    seqIdToResultMap.put(seqId, result);
    seqIdToRequestMap.put(seqId, request);
    send(serverId, location, seqId, request, update, result);
    return result;
  }

  /**
   * Update partition clock
   * @param serverId ps id
   * @param location ps location
   * @param partKey partition information
   * @param taskIndex task index
   * @param clock clock value
   * @return future result
   */
  public FutureResult<Response> updateClock(ParameterServerId serverId, Location location,
    PartitionKey partKey, int taskIndex, int clock) {
    int seqId = seqIdGen.incrementAndGet();
    UpdateClockRequest request = new UpdateClockRequest(partKey, taskIndex, clock);
    FutureResult<Response> response = new FutureResult<>();
    seqIdToResultMap.put(seqId, response);
    seqIdToRequestMap.put(seqId, request);

    // Serialize the request
    ByteBuf msg = ByteBufUtils.newByteBuf(16 + request.bufferLen(), useDirectBuf);
    msg.writeInt(-1);
    msg.writeInt(0);
    msg.writeInt(seqId);
    msg.writeInt(request.getType().getMethodId());
    request.serialize(msg);
    send(serverId, location, seqId, request, msg, response);
    return response;
  }

  private void changeSeqId(int seqId, ByteBuf update) {
    update.setInt(8, seqId);
  }

  private void returnChannel(Request item) {
    try {
      if (item.getContext().getChannelPool() != null && item.getContext().getChannel() != null) {
        item.getContext().getChannelPool().returnObject(item.getContext().getChannel());
        item.getContext().setChannelPool(null);
      }
    } catch (Exception x) {
      LOG.error("return channel to channel pool failed ", x);
    }
  }

  class RequesterChannelFutureListener implements ChannelFutureListener {
    private final Request request;
    private final int seqId;

    public RequesterChannelFutureListener(int seqId, Request request) {
      this.request = request;
      this.seqId = seqId;
    }

    @Override
    public void operationComplete(ChannelFuture future) throws Exception {
      LOG.debug("send request " + request + " with seqId=" + seqId + " complete");
      if (!future.isSuccess()) {
        LOG.error("send " + seqId + " failed ", future.cause());
        FutureResult<Response> result = seqIdToResultMap.remove(seqId);
        returnChannel(request);
        result.set(new Response(ResponseType.NETWORK_ERROR, "send request failed " + future.cause().toString()));
      }
    }
  }

  private GenericObjectPool<Channel> getChannelPool(Location loc) throws InterruptedException {
    return channelManager.getOrCreateChannelPool(new Location(loc.getIp(), loc.getPort() + 1));
  }
}
