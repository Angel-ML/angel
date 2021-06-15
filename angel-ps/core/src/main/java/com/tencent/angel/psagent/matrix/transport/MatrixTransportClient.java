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


package com.tencent.angel.psagent.matrix.transport;

import com.google.protobuf.ServiceException;
import com.tencent.angel.common.AngelThreadFactory;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.common.transport.ChannelManager2;
import com.tencent.angel.common.transport.ChannelPoolParam;
import com.tencent.angel.common.transport.NettyChannel;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.PartitionLocation;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.server.data.PSLocation;
import com.tencent.angel.ps.server.data.ServerState;
import com.tencent.angel.ps.server.data.TransportMethod;
import com.tencent.angel.ps.server.data.request.GetUDFRequest;
import com.tencent.angel.ps.server.data.request.Request;
import com.tencent.angel.ps.server.data.request.RequestContext;
import com.tencent.angel.ps.server.data.request.RequestHeader;
import com.tencent.angel.ps.server.data.request.UpdateUDFRequest;
import com.tencent.angel.ps.server.data.response.GetUDFResponse;
import com.tencent.angel.ps.server.data.response.Response;
import com.tencent.angel.ps.server.data.response.ResponseData;
import com.tencent.angel.ps.server.data.response.ResponseHeader;
import com.tencent.angel.ps.server.data.response.ResponseType;
import com.tencent.angel.ps.server.data.response.UpdateUDFResponse;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.adapter.UserRequest;
import com.tencent.angel.psagent.matrix.transport.adapter.UserRequestAdapter;
import com.tencent.angel.psagent.matrix.transport.response.GetRowHandler;
import com.tencent.angel.psagent.matrix.transport.response.GetRowsHandler;
import com.tencent.angel.psagent.matrix.transport.response.GetUDFHandler;
import com.tencent.angel.psagent.matrix.transport.response.Handler;
import com.tencent.angel.psagent.matrix.transport.response.IndexGetRowHandler;
import com.tencent.angel.psagent.matrix.transport.response.IndexGetRowsHandler;
import com.tencent.angel.psagent.matrix.transport.response.ResponseCache;
import com.tencent.angel.psagent.matrix.transport.response.UpdateHandler;
import com.tencent.angel.psagent.matrix.transport.response.UpdateUDFHandler;
import com.tencent.angel.utils.ByteBufUtils;
import com.tencent.angel.utils.StringUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.hadoop.conf.Configuration;

/**
 * RPC client to parameter servers. It uses Netty as the network communication framework.
 */
public class MatrixTransportClient implements MatrixTransportInterface {

  private static final Log LOG = LogFactory.getLog(MatrixTransportClient.class);

  /**
   * partition request id to request context map
   */
  private final ConcurrentHashMap<Integer, Request> seqIdToRequestMap;

  /**
   * request to result map
   */
  @SuppressWarnings("rawtypes")
  private final ConcurrentHashMap<Request, FutureResult>
      requestToResultMap;

  /**
   * netty client bootstrap
   */
  private Bootstrap bootstrap;

  /**
   * netty client thread pool
   */
  private EventLoopGroup eventGroup;

  /**
   * un-decoded result queue
   */
  private final LinkedBlockingQueue<ByteBuf> msgQueue;

  /**
   * stop the all services in the rpc client
   */
  private final AtomicBoolean stopped;

  /**
   * event dispatcher for matrix transport client
   */
  private RequestDispatcher requestDispacher;

  /**
   * event queue
   */
  private final LinkedBlockingQueue<DispatcherEvent> dispatchMessageQueue;

  /**
   * ps id to partition get requests map
   */
  private final ConcurrentHashMap<ParameterServerId, LinkedBlockingQueue<Request>> getItemQueues;

  /**
   * ps id to partition put requests map
   */
  private final ConcurrentHashMap<ParameterServerId, LinkedBlockingQueue<Request>> putItemQueues;

  /**
   * timed event generator, it used to check there are failed partition requests we should
   * re-dispatch periodically
   */
  private final Timer timer;

  /**
   * timed event generation interval in milliseconds
   */
  private final int checkPeriodMS;

  /**
   * retry interval in milliseconds for failed requests
   */
  private final int retryIntervalMs;

  /**
   * Maximun try number for a single RPC
   */
  private final int maxTryNum;

  /**
   * client worker pool: 1.use to deserialize partition responses and merge them to final result
   * 2.use to generate partition request and serialize it
   */
  private ExecutorService requestThreadPool;

  /**
   * client worker pool: 1.use to deserialize partition responses and merge them to final result
   * 2.use to generate partition request and serialize it
   */
  private ExecutorService hbThreadPool;

  /**
   * client worker pool: 1.use to deserialize partition responses and merge them to final result
   * 2.use to generate partition request and serialize it
   */
  private ExecutorService responseThreadPool;

  /**
   * response message handler
   */
  private ResponseDispatcher responseDispatcher;

  private final ArrayList<Long> getUseTimes;

  /**
   * channel pool manager:it maintain a channel pool for every server
   */
  private ChannelManager2 channelManager;

  /**
   * use direct netty buffer or not
   */
  private final boolean useDirectBuffer;

  /**
   * use pooled netty buffer or not
   */
  private final boolean usePool;

  private final boolean disableRouterCache;

  private final int partReplicNum;

  private final int hbTimeoutMS;

  /**
   * PSAgent to PS heartbeat sender
   */
  private volatile PSHeartbeat psHeartbeater;

  /**
   * PS location refresher
   */
  private volatile PSLocRefresher psLocRefresher;

  /**
   * PS to last heartbeat timestamp map, it only contains the PSS that some RPCS failed
   */
  private final ConcurrentHashMap<ParameterServerId, GrayServer> grayServers =
      new ConcurrentHashMap<>();

  /**
   * Failed PS to old loc map
   */
  private final ConcurrentHashMap<ParameterServerId, Location> failedPSToLocMap =
      new ConcurrentHashMap<>();

  /**
   * Request id to request send result map
   */
  private final ConcurrentHashMap<Integer, SendResultKey> seqIdToSendCFMap =
      new ConcurrentHashMap<>();

  /**
   * Request id generator
   */
  private final AtomicInteger currentSeqId = new AtomicInteger(0);

  /**
   * PS id to PS state map
   */
  private final ConcurrentHashMap<ParameterServerId, ServerState> psIdToStateMap =
      new ConcurrentHashMap<>();

  /**
   * RPC running context
   */
  private final RPCContext rpcContext;

  /**
   * PS location to last get channel timestamp map
   */
  private final ConcurrentHashMap<PSLocation, GetChannelContext> psLocToGetChannelContextMap =
      new ConcurrentHashMap<>();

  /**
   * PS location to "no active channel" error counter map
   */
  private final ConcurrentHashMap<PSLocation, AtomicInteger> psLocToNoActiveCounterMap =
      new ConcurrentHashMap<>();

  private final ConcurrentHashMap<TransportMethod, Handler> responseHanders;

  /**
   * Create a new MatrixTransportClient.
   */
  @SuppressWarnings("rawtypes")
  public MatrixTransportClient() {
    seqIdToRequestMap = new ConcurrentHashMap<>();
    requestToResultMap = new ConcurrentHashMap<>();

    dispatchMessageQueue = new LinkedBlockingQueue<>();
    getItemQueues = new ConcurrentHashMap<>();
    putItemQueues = new ConcurrentHashMap<>();
    responseHanders = new ConcurrentHashMap<>();

    getUseTimes = new ArrayList<>();

    msgQueue = new LinkedBlockingQueue<>();
    stopped = new AtomicBoolean(false);

    Configuration conf = PSAgentContext.get().getConf();
    timer = new Timer();
    checkPeriodMS = conf.getInt(AngelConf.ANGEL_MATRIXTRANSFER_CHECK_INTERVAL_MS,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_CHECK_INTERVAL_MS);

    retryIntervalMs = conf.getInt(AngelConf.ANGEL_MATRIXTRANSFER_RETRY_INTERVAL_MS,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_RETRY_INTERVAL_MS);

    maxTryNum = conf.getInt(AngelConf.ANGEL_MATRIXTRANSFER_MAX_TRY_COUNTER,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_MAX_TRY_COUNTER);

    useDirectBuffer = conf.getBoolean(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_USEDIRECTBUFFER,
        AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_USEDIRECTBUFFER);

    usePool = conf.getBoolean(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_USEPOOL,
        AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_USEPOOL);

    hbTimeoutMS = conf.getInt(AngelConf.ANGEL_CLIENT_HEARTBEAT_INTERVAL_TIMEOUT_MS,
        AngelConf.DEFAULT_ANGEL_CLIENT_HEARTBEAT_INTERVAL_TIMEOUT_MS);

    ByteBufUtils.useDirect = useDirectBuffer;
    ByteBufUtils.usePool = usePool;

    partReplicNum = conf.getInt(AngelConf.ANGEL_PS_HA_REPLICATION_NUMBER,
        AngelConf.DEFAULT_ANGEL_PS_HA_REPLICATION_NUMBER);
    disableRouterCache = partReplicNum > 1;

    channelManager = null;
    rpcContext = new RPCContext();
  }

  private void init() {
    Configuration conf = PSAgentContext.get().getConf();

    rpcContext.init(conf, PSAgentContext.get().getLocationManager().getPsIds());

    // Init response handler
    registeHandler();

    // Init network parameters
    int nettyWorkerNum = conf
        .getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_EVENTGROUP_THREADNUM,
            AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_EVENTGROUP_THREADNUM);

    int sendBuffSize = conf.getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_SNDBUF,
        AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_SNDBUF);

    int recvBuffSize = conf.getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_RCVBUF,
        AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_RCVBUF);

    final int maxMessageSize = conf.getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_MAX_MESSAGE_SIZE,
        AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_MAX_MESSAGE_SIZE);

    requestThreadPool = Executors.newFixedThreadPool(conf
        .getInt(AngelConf.ANGEL_MATRIXTRANSFER_CLIENT_REQUESTER_POOL_SIZE,
            AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_CLIENT_REQUESTER_POOL_SIZE), new AngelThreadFactory("RPCRequest"));

    responseThreadPool = Executors.newFixedThreadPool(conf
        .getInt(AngelConf.ANGEL_MATRIXTRANSFER_CLIENT_RESPONSER_POOL_SIZE,
            AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_CLIENT_RESPONSER_POOL_SIZE), new AngelThreadFactory("RPCResponser"));

    ChannelPoolParam poolParam = new ChannelPoolParam();
    poolParam.maxActive = conf
        .getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_MAX_CONN_PERSERVER,
            AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_MAX_CONN_PERSERVER);

    poolParam.minActive = conf
        .getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_MIN_CONN_PERSERVER,
            AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_MIN_CONN_PERSERVER);

    poolParam.maxIdleTimeMs = conf
        .getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_MAX_CONN_IDLETIME_MS,
            AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_MAX_CONN_IDLETIME_MS);

    poolParam.getChannelTimeoutMs = conf
        .getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_GET_CONN_TIMEOUT_MS,
            AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_GET_CONN_TIMEOUT_MS);

    int ioRatio = conf.getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_IORATIO,
        AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_IORATIO);

    String channelType = conf.get(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_CHANNEL_TYPE,
        AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_CHANNEL_TYPE);

    hbThreadPool = Executors.newFixedThreadPool(8, new AngelThreadFactory("Heartbeat"));

    bootstrap = new Bootstrap();
    channelManager = new ChannelManager2(bootstrap, poolParam);
    channelManager.initAndStart();

    // Use Epoll for linux
    Class channelClass;
    String os = System.getProperty("os.name");
    if (os != null && os.toLowerCase().startsWith("linux") && channelType.equals("epoll")) {
      LOG.info("Use epoll channel");
      channelClass = EpollSocketChannel.class;
      eventGroup = new EpollEventLoopGroup(nettyWorkerNum);
      ((EpollEventLoopGroup) eventGroup).setIoRatio(ioRatio);
    } else {
      LOG.info("Use nio channel");
      channelClass = NioSocketChannel.class;
      eventGroup = new NioEventLoopGroup(nettyWorkerNum);
      ((NioEventLoopGroup) eventGroup).setIoRatio(ioRatio);
    }

    MatrixTransportClient client = this;
    bootstrap.group(eventGroup).channel(channelClass).option(ChannelOption.SO_SNDBUF, sendBuffSize)
        .option(ChannelOption.SO_RCVBUF, recvBuffSize).option(ChannelOption.SO_KEEPALIVE, true)
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeLine = ch.pipeline();
            pipeLine.addLast(new LengthFieldBasedFrameDecoder(maxMessageSize, 0, 4, 0, 4));
            pipeLine.addLast(new LengthFieldPrepender(4));
            pipeLine
                .addLast(
                    new MatrixTransportClientHandler(client, dispatchMessageQueue, rpcContext));
          }
        });
  }

  private void registeHandler() {
    PSAgentContext context = PSAgentContext.get();
    responseHanders.put(TransportMethod.GET_ROWSPLIT, new GetRowHandler(context));
    responseHanders.put(TransportMethod.GET_ROWSSPLIT, new GetRowsHandler(context));
    responseHanders.put(TransportMethod.INDEX_GET_ROW, new IndexGetRowHandler(context));
    responseHanders.put(TransportMethod.INDEX_GET_ROWS, new IndexGetRowsHandler(context));
    responseHanders.put(TransportMethod.UPDATE, new UpdateHandler(context));
    responseHanders.put(TransportMethod.GET_PSF, new GetUDFHandler(context));
    responseHanders.put(TransportMethod.UPDATE_PSF, new UpdateUDFHandler(context));
  }

  /**
   * Start the task dispatcher, rpc responses handler and the time clock generator.
   */
  public void start() {
    init();

    psHeartbeater = new PSHeartbeat();
    psHeartbeater.setName("ps-heartbeat");
    psHeartbeater.start();

    psLocRefresher = new PSLocRefresher();
    psLocRefresher.setName("psloc-refresher");
    psLocRefresher.start();

    requestDispacher = new RequestDispatcher();
    requestDispacher.setName("request-dispatcher");
    requestDispacher.start();

    responseDispatcher = new ResponseDispatcher();
    responseDispatcher.setName("response-dispatcher");
    responseDispatcher.start();

    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          periodCheck();
        } catch (Throwable e) {
          LOG.error("Timer error ", e);
        }
      }
    }, 100, checkPeriodMS);
  }

  /**
   * Stop all services and worker threads.
   */
  public void stop() {
    if (!stopped.getAndSet(true)) {
      if (timer != null) {
        timer.cancel();
      }

      if (requestDispacher != null) {
        requestDispacher.interrupt();
        requestDispacher = null;
      }

      if (responseDispatcher != null) {
        responseDispatcher.interrupt();
        responseDispatcher = null;
      }

      if (channelManager != null) {
        channelManager.stop();
      }

      if (eventGroup != null) {
        eventGroup.shutdownGracefully();
      }

      if (requestThreadPool != null) {
        requestThreadPool.shutdownNow();
      }

      if (responseThreadPool != null) {
        responseThreadPool.shutdownNow();
      }
    }
  }

  private void periodCheck() throws InterruptedException {
    try {
      dispatchMessageQueue.put(new DispatcherEvent(EventType.PERIOD_CHECK));
    } catch (Throwable x) {
      LOG.error("put PERIOD_CHECK event to queue failed ", x);
    }
  }

  private void addToGetQueueForServer(ParameterServerId serverId, Request request) {
    request.setContext(new RequestContext());
    request.getContext().setServerId(serverId);
    LinkedBlockingQueue<Request> queue = getItemQueues.get(serverId);
    if (queue == null) {
      queue = new LinkedBlockingQueue<>();
      getItemQueues.putIfAbsent(serverId, queue);
      queue = getItemQueues.get(serverId);
    }
    queue.add(request);
  }

  private void addToPutQueueForServer(ParameterServerId serverId, Request request) {
    request.setContext(new RequestContext());
    request.getContext().setServerId(serverId);
    LinkedBlockingQueue<Request> queue = putItemQueues.get(serverId);
    if (queue == null) {
      queue = new LinkedBlockingQueue<>();
      putItemQueues.putIfAbsent(serverId, queue);
      queue = putItemQueues.get(serverId);
    }
    queue.add(request);
  }

  @Override
  public void sendGetRequest(Request request) {
    ParameterServerId serverId = PSAgentContext.get().getMatrixMetaManager().getMasterPS(
        request.getHeader().getMatrixId(), request.getHeader().getPartId());
    addToGetQueueForServer(serverId, request);
    startGet();
  }

  @Override
  public void sendUpdateRequest(Request request) {
    ParameterServerId serverId = PSAgentContext.get().getMatrixMetaManager().getMasterPS(
        request.getHeader().getMatrixId(), request.getHeader().getPartId());
    addToPutQueueForServer(serverId, request);
    startPut();
  }

  @Override
  public FutureResult<PartitionGetResult> get(GetFunc func, PartitionGetParam param) {
    // Request header
    RequestHeader header = createRequestHeader(-1, TransportMethod.GET_PSF,
        param.getMatrixId(), param.getPartKey().getPartitionId());

    // Request body
    GetUDFRequest requestData = new GetUDFRequest(func.getClass().getName(), param);

    // Request
    Request request = new Request(header, requestData);

    FutureResult<PartitionGetResult> resultFuture = new FutureResult<>();
    requestToResultMap.put(request, resultFuture);

    // Send the request
    sendGetRequest(request);

    return resultFuture;
  }

  @Override
  public FutureResult<VoidResult> update(UpdateFunc func, PartitionUpdateParam param) {
    // Request header
    RequestHeader header = createRequestHeader(-1, TransportMethod.UPDATE_PSF,
        param.getMatrixId(), param.getPartKey().getPartitionId());

    // Request body
    UpdateUDFRequest requestData = new UpdateUDFRequest(func.getClass().getName(), param);

    // Request
    Request request = new Request(header, requestData);

    FutureResult<VoidResult> resultFuture = new FutureResult<>();
    requestToResultMap.put(request, resultFuture);

    // Send the request
    sendUpdateRequest(request);

    return resultFuture;
  }

  private RequestHeader createRequestHeader(int requestId, TransportMethod method, int matrixId, int partId) {
    RequestHeader header = new RequestHeader();
    header.setUserRequestId(requestId);
    header.setMethodId(method.getMethodId());
    header.setMethodId(matrixId);
    header.setPartId(partId);
    return header;
  }

  class PSLocRefresher extends Thread {

    @Override
    public void run() {
      long lastHbTs = System.currentTimeMillis();
      while (!stopped.get() && !Thread.interrupted()) {

        if(System.currentTimeMillis() - lastHbTs > hbTimeoutMS) {
          LOG.fatal("can not connect to master in " + hbTimeoutMS + " ms!!");
          PSAgentContext.get().getUserRequestAdapter().clear("Angel master is exit!!");
        }

        if(!failedPSToLocMap.isEmpty()) {
          Iterator<Entry<ParameterServerId, Location>> iter = failedPSToLocMap.entrySet().iterator();

          while (iter.hasNext()) {
            Entry<ParameterServerId, Location> entry = iter.next();
            try {
              Location loc = PSAgentContext.get().getMasterClient().getPSLocation(entry.getKey());
              lastHbTs = System.currentTimeMillis();
              if (loc != null && !loc.equals(entry.getValue())) {
                LOG.info("Refresh location for PS " + entry.getKey() + ", new location=" + loc);
                // Update location table
                PSAgentContext.get().getLocationManager().setPsLocation(entry.getKey(), loc);

                // Remove the server from failed list
                removeFailedServers(entry.getKey());

                // Notify refresh success message to request dispatcher
                refreshServerLocationSuccess(entry.getKey(), true);
              }
            } catch (Throwable e) {
              LOG.error("Get location from master failed ", e);
            }
          }
        } else {
          lastHbTs = System.currentTimeMillis();
        }

        try {
          Thread.sleep(5000);
        } catch (Throwable e) {
          if (!stopped.get()) {
            LOG.error("ps-heartbeat is interruptted");
          }
        }
      }
    }
  }

  private boolean isRefreshing(ParameterServerId psId) {
    return failedPSToLocMap.containsKey(psId);
  }

  private void removeFailedServers(ParameterServerId psId) {
    failedPSToLocMap.remove(psId);
  }

  class PSHeartbeat extends Thread {

    /**
     * PS heartbeat timeout in milliseconds
     */
    private final int psHbTimeOutMS;

    /**
     * PS heartbeat interval in milliseconds
     */
    private final int psHbTimeIntervalMS;

    public PSHeartbeat() {
      Configuration conf = PSAgentContext.get().getConf();
      psHbTimeIntervalMS = conf.getInt(AngelConf.ANGEL_PSAGENT_TO_PS_HEARTBEAT_INTERVAL_MS,
          AngelConf.DEFAULT_ANGEL_PSAGENT_TO_PS_HEARTBEAT_INTERVAL_MS);

      psHbTimeOutMS = conf.getInt(AngelConf.ANGEL_PSAGENT_TO_PS_HEARTBEAT_TIMEOUT_MS,
          AngelConf.DEFAULT_ANGEL_PSAGENT_TO_PS_HEARTBEAT_TIMEOUT_MS);
    }

    @Override
    public void run() {
      while (!stopped.get() && !Thread.interrupted()) {
        Iterator<Entry<ParameterServerId, GrayServer>> iter = grayServers.entrySet().iterator();
        Entry<ParameterServerId, GrayServer> entry;
        while (iter.hasNext()) {
          entry = iter.next();
          LOG.warn("PS " + entry.getKey() + " is in gray server list, Send hb to it to check");

          // Check PS exist or not
          if (isPSExited(entry.getValue())) {
            LOG.error("PS " + entry.getValue().psLoc + " already exited, just remove from gray"
                + " list and put it to failed list");
            try {
              notifyServerFailed(entry.getValue(), false);
            } catch (ServiceException e) {
              continue;
            }
            iter.remove();
            continue;
          }

          // Check heartbeat timeout
          if (isTimeOut(entry.getValue())) {
            LOG.error("PS " + entry.getValue().psLoc + " heartbeat timeout, notify to Master");
            try {
              notifyServerFailed(entry.getValue(), true);
            } catch (ServiceException e) {
              LOG.error("Notify PS " + entry.getValue().psLoc + " failed message to Master failed ",
                  e);
              continue;
            }
            iter.remove();
            continue;
          }

          if (shouldRemoveFromGrayList(entry.getValue())) {
            LOG.debug("PS " + entry.getValue().psLoc
                + " state back to normal, remove it from gray server list");
            iter.remove();
            notifyServerNormal(entry.getValue());
            continue;
          }

          //getPSState(entry.getKey());
          sendHeartBeat(entry.getKey(), entry.getValue().psLoc);
        }
        try {
          Thread.sleep(psHbTimeIntervalMS);
        } catch (Throwable e) {
          if (!stopped.get()) {
            LOG.error("ps-heartbeat is interruptted");
          }
        }
      }
    }

    private boolean isTimeOut(GrayServer server) {
      return System.currentTimeMillis() - server.lastHBTs > psHbTimeOutMS;
    }

    private boolean isPSExited(GrayServer server) {
      return server.state == ServerState.EXITED;
    }

    private boolean shouldRemoveFromGrayList(GrayServer server) {
      return server.state != null && (server.state == ServerState.IDLE
          || server.state == ServerState.GENERAL);
    }
  }

  private void notifyServerFailed(GrayServer server, boolean notifyToMaster)
      throws ServiceException {
    try {
      dispatchMessageQueue.put(new ServerEvent(EventType.SERVER_FAILED, server.psLoc));
    } catch (Exception e) {
      LOG.error("add SERVER_FAILED event for request failed, ", e);
    }

    if (notifyToMaster) {
      PSAgentContext.get().getMasterClient().psFailed(server.psLoc);
    }
  }

  private void notifyServerNormal(GrayServer server) {
    psIdToStateMap.put(server.psLoc.psId, server.state);
    try {
      dispatchMessageQueue.put(new ServerEvent(EventType.SERVER_NORMAL, server.psLoc));
    } catch (Exception e) {
      LOG.error("add SERVER_NORMAL event for request failed, ", e);
    }
  }

  private void sendHeartBeat(ParameterServerId psId, PSLocation psLoc) {
    hbThreadPool.execute(() -> {
      try {
        LOG.debug("Start to send hb to " + psLoc);
        ServerState state =
            PSAgentContext.get().getPSControlClientManager().getOrCreatePSClient(psLoc.loc)
                .getState();

        GrayServer server = grayServers.get(psId);
        if (server != null) {
          server.state = state;
          server.lastHBTs = System.currentTimeMillis();
        }
      } catch (Throwable e) {
        LOG.error("Send heartbeat to " + psLoc + " failed ", e);

        // Check PS restart or not
        try {
          if (PSAgentContext.get().getMasterClient().isPSExited(psLoc)) {
            GrayServer server = grayServers.get(psLoc.psId);
            if (server != null) {
              server.state = ServerState.EXITED;
              server.lastHBTs = System.currentTimeMillis();
            }
          }
        } catch (Throwable x) {

        }
      }
    });
  }

  private int genSeqId() {
    return currentSeqId.incrementAndGet();
  }

  private boolean isFailed(ParameterServerId psId) {
    return failedPSToLocMap.containsKey(psId);
  }

  private boolean isInGrayList(ParameterServerId psId) {
    return grayServers.containsKey(psId);
  }

  private void removeFromGrayList(ParameterServerId psId) {
    grayServers.remove(psId);
  }

  private void addToGrayList(PSLocation psLoc) {
    grayServers.putIfAbsent(psLoc.psId, new GrayServer(psLoc, System.currentTimeMillis()));
  }

  /**
   * RPC request dispatcher.
   */
  class RequestDispatcher extends Thread {

    /**
     * schedulable failed put request queue: the requests in the will be scheduled first
     */
    private final LinkedBlockingQueue<Request> schedulableFailedPutCache;

    /**
     * schedulable failed get request queue: the requests in the will be scheduled first
     */
    private final LinkedBlockingQueue<Request> schedulableFailedGetCache;

    /**
     * un-schedulable failed partition request queues: the requests in these queues will be move to
     * schedulable queues if need retry or server locations has been refreshed
     */
    private final LinkedList<Request> failedPutCache;
    private final LinkedList<Request> failedGetCache;

    /**
     * server ids and last choose server indexes
     */
    private final ParameterServerId[] psIds;

    /**
     * the index of server that last schedule put request send to
     */
    private int lastChosenPutServerIndex;

    /**
     * the index of server that last schedule get request send to
     */
    public int lastChosenGetServerIndex;

    /**
     * refreshing server set:the partition request for refreshing server can not be scheduled
     */
    //private final HashSet<ParameterServerId> refreshingServerSet;

    private final int requestTimeOut;

    private final int psHbTimeOutMS;

    private int tickClock;

    public RequestDispatcher() {
      Configuration conf = PSAgentContext.get().getConf();
      psIds = PSAgentContext.get().getLocationManager().getPsIds();
      lastChosenPutServerIndex = new Random().nextInt(psIds.length);
      lastChosenGetServerIndex = new Random().nextInt(psIds.length);

      failedPutCache = new LinkedList<>();
      failedGetCache = new LinkedList<>();
      schedulableFailedPutCache = new LinkedBlockingQueue<>();
      schedulableFailedGetCache = new LinkedBlockingQueue<>();

      requestTimeOut = conf.getInt(AngelConf.ANGEL_MATRIXTRANSFER_REQUEST_TIMEOUT_MS,
          AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_REQUEST_TIMEOUT_MS);

      psHbTimeOutMS = conf.getInt(AngelConf.ANGEL_PSAGENT_TO_PS_HEARTBEAT_TIMEOUT_MS,
          AngelConf.DEFAULT_ANGEL_PSAGENT_TO_PS_HEARTBEAT_TIMEOUT_MS);

      tickClock = 0;
      LOG.info("ByteOrder.nativeOrder()=" + ByteOrder.nativeOrder());
    }

    @Override
    public void run() {
      while (!stopped.get() && !Thread.interrupted()) {
        try {
          DispatcherEvent event = dispatchMessageQueue.take();
          LOG.debug("handler event " + event);
          switch (event.getType()) {
            case START_GET: {
              getDataSplit();
              break;
            }

            case GET_SUCCESS: {
              Request request = ((RequestDispatchEvent) event).getRequest();
              getDataSplit();
              break;
            }

            case GET_FAILED: {
              RequestFailedEvent getFailedEvent = (RequestFailedEvent) event;
              Request request = getFailedEvent.getRequest();
              request.getContext().setFailedTs(System.currentTimeMillis());

              // Add it to failed rpc list
              request.getContext().setNextRetryTs(genNextRetryTs());
              failedGetCache.add(request);

              // Add the server to gray server list
              if (needPutToGrayServers(getFailedEvent.getFailedType())) {
                addToGrayList(new PSLocation(request.getContext().getActualServerId(),
                    request.getContext().getLocation()));
              }
              getDataSplit();
              break;
            }

            case START_PUT: {
              putDataSplit();
              break;
            }

            case PUT_SUCCESS: {
              Request request = ((RequestDispatchEvent) event).getRequest();
              putDataSplit();
              break;
            }

            case PUT_FAILED: {
              RequestFailedEvent putFailedEvent = (RequestFailedEvent) event;
              Request request = putFailedEvent.getRequest();
              request.getContext().setFailedTs(System.currentTimeMillis());

              // Add it to failed rpc list
              request.getContext().setNextRetryTs(genNextRetryTs());
              failedPutCache.add(request);

              // Add the server to gray server list
              if (needPutToGrayServers(putFailedEvent.getFailedType())) {
                addToGrayList(new PSLocation(request.getContext().getActualServerId(),
                    request.getContext().getLocation()));
              }

              putDataSplit();
              break;
            }

            case SERVER_FAILED: {
              ServerEvent serverFailedEvent = (ServerEvent) event;

              // Move from gray server list to failed server list
              failedPSToLocMap
                  .put(serverFailedEvent.getPsLoc().psId, serverFailedEvent.getPsLoc().loc);
              grayServers.remove(serverFailedEvent.getPsLoc().psId);

              // Handle the RPCS to this server
              psFailed(serverFailedEvent.getPsLoc());
              break;
            }

            case SERVER_NORMAL: {
              ServerEvent serverNormalEvent = (ServerEvent) event;
              grayServers.remove(serverNormalEvent.getPsLoc().psId);
              psNormal(serverNormalEvent.getPsLoc());
              break;
            }

            case REFRESH_SERVER_LOCATION_SUCCESS: {
              RefreshServerLocationEvent refreshEvent = (RefreshServerLocationEvent) event;
              ParameterServerId serverId = refreshEvent.getServerId();
              LOG.info("refresh location for server " + serverId + " success ");

              failedPSToLocMap.remove(serverId);
              dispatchTransportEvent(serverId);
              break;
            }

            case PERIOD_CHECK: {
              tickClock++;
              dispatchTransportEvent(null);
              break;
            }

            case CHANNEL_CLOSED: {
              removeRequestForChannel(((ChannelClosedEvent) event).getChannel());
              break;
            }

            default:
              break;
          }
        } catch (InterruptedException e) {
          if (!stopped.get()) {
            LOG.fatal("RequestDispatcher is interrupted!! ", e);
            PSAgentContext.get().getPsAgent().error("RequestDispatcher is interrupted!!");
          }
        } catch (Throwable e) {
          LOG.fatal("RequestDispatcher is failed ", e);
          PSAgentContext.get().getPsAgent().error("RequestDispatcher is failed. " + e.getMessage());
        }
      }
    }

    private long genNextRetryTs() {
      long startTs = System.currentTimeMillis();
      Random r = new Random();
      return startTs + (long) (requestTimeOut * r.nextDouble());
    }

    private boolean needPutToGrayServers(ResponseType type) {
      return type == ResponseType.NETWORK_ERROR || type == ResponseType.SERVER_IS_BUSY
          || type == ResponseType.TIMEOUT || type == ResponseType.CONNECT_REFUSED;
    }

    /**
     * choose get partition requests and send it to server first schedule schedulableFailedGetCache
     */
    private void getDataSplit() {
      // Submit the schedulable failed get RPCS
      submitTask(schedulableFailedGetCache);
      if (!schedulableFailedGetCache.isEmpty()) {
        return;
      }

      // Submit new get RPCS
      LinkedBlockingQueue<Request> getQueue;
      while (true) {
        getQueue = chooseGetQueue();
        if (getQueue == null) {
          return;
        }

        // if submit task in getQueue failed, we should make up the last chosen get queue index
        if (!getQueue.isEmpty() && (submitTask(getQueue) == 0)) {
          makeUpChoosedGetQueue();
          return;
        }
      }
    }

    private void makeUpChoosedGetQueue() {
      lastChosenGetServerIndex -= 1;
      if (lastChosenGetServerIndex < 0) {
        lastChosenGetServerIndex = psIds.length - 1;
      }
    }

    /**
     * choose a schedule get queue, now use Round-Robin default
     *
     * @return the chosen queue to be scheduled
     */
    private LinkedBlockingQueue<Request> chooseGetQueue() {
      LinkedBlockingQueue<Request> retQueue;
      int index = (lastChosenGetServerIndex + 1) % psIds.length;
      int maxCheckTime = psIds.length;

      while (maxCheckTime-- > 0) {
        if (!isInGrayList(psIds[index]) && !isFailed(psIds[index]) && !checkIsOverReqNumLimit(
            psIds[index])) {
          retQueue = getItemQueues.get(psIds[index]);
          if (retQueue != null && !retQueue.isEmpty()) {
            lastChosenGetServerIndex = index;
            return retQueue;
          }
        }

        index++;
        if (index >= psIds.length) {
          index = 0;
        }
      }

      return null;
    }

    /**
     * submit get partition requests in a queue, if a request in this queue satisfy flow-control
     * condition and the server location for this request is not refreshing, it can be submit
     *
     * @param queue get request queue
     * @return submit request number
     */
    private int submitTask(LinkedBlockingQueue<Request> queue) {
      // If the queue is empty, just return 0
      if (queue.isEmpty()) {
        return 0;
      }

      Request item;
      int submitNum = 0;
      while ((item = queue.poll()) != null) {
        if (!checkIsOverReqNumLimit(item.getContext().getServerId())) {
          // If request is not over limit, just submit it
          submit(item);
          submitNum++;
        } else {
          queue.add(item);
          break;
        }
      }

      return submitNum;
    }

    private boolean checkIsOverReqNumLimit(ParameterServerId serverId) {
      return ((rpcContext.getInflightRPCCounters() + 1) > rpcContext.getMaxInflightRPCNum()) || (
          (rpcContext.getServerInflightRPCCounters(serverId) + 1) > rpcContext
              .getMaxInflightRPCNumPerServer());
    }

    /**
     * choose put partition requests and send it to server first schedule schedulableFailedGetCache
     */
    private void putDataSplit() {
      // Submit the schedulable failed get RPCS
      submitTask(schedulableFailedPutCache);
      if (!schedulableFailedPutCache.isEmpty()) {
        return;
      }

      // Submit new put RPCS
      LinkedBlockingQueue<Request> putQueue = null;
      while (true) {
        putQueue = choosePutQueue();
        if (putQueue == null) {
          return;
        }

        // if submit task in getQueue failed, we should make up the last chosen get queue index
        if (!putQueue.isEmpty() && submitTask(putQueue) == 0) {
          makeUpChoosedPutQueue();
          return;
        }
      }
    }

    private void makeUpChoosedPutQueue() {
      lastChosenPutServerIndex -= 1;
      if (lastChosenPutServerIndex < 0) {
        lastChosenPutServerIndex = psIds.length - 1;
      }
    }

    private void submit(Request item) {
      int seqId = currentSeqId.incrementAndGet();
      item.setSeqId(seqId);
      item.getContext().setWaitTimeTicks(0);
      rpcContext.before(item.getContext().getServerId());
      LOG.debug("submit request seqId=" + seqId + ",request=" + item);
      seqIdToRequestMap.put(seqId, item);
      requestThreadPool.execute(new Requester(item, seqId));
    }

    /**
     * choose a schedule put queue, now use Round-Robin default
     *
     * @return the chosen queue to be scheduled
     */
    private LinkedBlockingQueue<Request> choosePutQueue() {
      LinkedBlockingQueue<Request> retQueue;
      int index = (lastChosenPutServerIndex + 1) % psIds.length;
      int maxCheckTime = psIds.length;

      while (maxCheckTime-- > 0) {
        if (!isRefreshing(psIds[index]) && !checkIsOverReqNumLimit(psIds[index])) {
          retQueue = putItemQueues.get(psIds[index]);
          if (retQueue != null && !retQueue.isEmpty()) {
            // LOG.info("choose put server " + psIds[index]);
            lastChosenPutServerIndex = index;
            return retQueue;
          }
        }

        index++;
        if (index >= psIds.length) {
          index = 0;
        }
      }

      return null;
    }

    private void printDispatchInfo() {
      for (Entry<ParameterServerId, LinkedBlockingQueue<Request>> entry : getItemQueues
          .entrySet()) {
        LOG.info(
            "period check, for server " + entry.getKey() + ", there is " + entry.getValue().size()
                + " get items need dispatch ");
      }

      for (Entry<ParameterServerId, LinkedBlockingQueue<Request>> entry : putItemQueues
          .entrySet()) {
        LOG.info(
            "period check, for server " + entry.getKey() + ", there is " + entry.getValue().size()
                + " put items need dispatch ");
      }

      LOG.info("schedulableFailedGetCache size = " + schedulableFailedGetCache.size());
      LOG.info("schedulableFailedPutCache size = " + schedulableFailedPutCache.size());

      for (Entry<Integer, Request> entry : seqIdToRequestMap.entrySet()) {
        LOG.info("infight request seqId=" + entry.getKey() + ", request context=" + entry.getValue()
            + ", request channel=" + entry.getValue().getContext().getChannel());
      }

      rpcContext.print();
    }

    private void dispatchTransportEvent(ParameterServerId serverId) {
      if (tickClock % 10 != 0) {
        return;
      }

      // Check all pending RPCS
      checkTimeout();

      // Check get channel context
      if (tickClock % 100 == 0) {
        checkChannelContext();
      }

      // Check all failed PUT RPCS and put it to schedulable list for re-schedule
      long ts = System.currentTimeMillis();
      Iterator<Request> iter = failedPutCache.iterator();
      ParameterServerId requestServerId;
      Request item;
      while (iter.hasNext()) {
        item = iter.next();
        requestServerId = item.getContext().getServerId();
        if ((serverId == null || requestServerId == serverId) && !isInGrayList(requestServerId)
            && !isFailed(requestServerId) && (ts - item.getContext().getNextRetryTs() > 0)) {
          //&& (ts - item.getContext().getFailedTs() >= retryIntervalMs)) {
          schedulableFailedPutCache.add(item);
          iter.remove();
        }
      }
      putDataSplit();

      // Check all failed PUT RPCS and put it to schedulable list for re-schedule
      iter = failedGetCache.iterator();
      while (iter.hasNext()) {
        item = iter.next();
        requestServerId = item.getContext().getServerId();
        if ((serverId == null || requestServerId == serverId) && !isInGrayList(
            item.getContext().getServerId()) && !isFailed(requestServerId) && (
            ts - item.getContext().getNextRetryTs() > 0)) {
          //&& (ts - item.getContext().getFailedTs() >= retryIntervalMs)) {
          schedulableFailedGetCache.add(item);
          iter.remove();
        }
      }
      getDataSplit();
      if ((tickClock % 100 == 0) && LOG.isDebugEnabled()) {
        printDispatchInfo();
        //channelManager.printPools();
      }
    }

    private void checkChannelContext() {
      long startTs = System.currentTimeMillis();
      for (Entry<PSLocation, GetChannelContext> entry : psLocToGetChannelContextMap.entrySet()) {
        if ((entry.getValue().getInactiveCounter() > 0) || (
            (startTs - entry.getValue().getLastCheckTs() > requestTimeOut * 2) && (
                entry.getValue().getSuccessCounter() == 0 && entry.getValue().failedCounter > 0))) {
          LOG.error("Channel for ps " + entry.getKey() + " noactive channel happened time:" + entry
              .getValue().getInactiveCounter() + ", success time:" + entry.getValue().successCounter
              + ", failed time:" + entry.getValue().failedCounter);
          closeChannels(entry.getKey());
          entry.getValue().reset();
          continue;
        }
      }
    }

    private void checkTimeout() {
      try {
        long ts = System.currentTimeMillis();
        for (Entry<Integer, Request> entry : seqIdToRequestMap.entrySet()) {
          Request item = entry.getValue();
          if (item.timeoutEnable() && item.getContext().getSendStartTs() > 0 && (
              (ts - item.getContext().getSendStartTs())
                  > requestTimeOut)) {
            LOG.error("Request " + item + " to PS " + item.getContext().getActualServerId()
                + " not return result over " + requestTimeOut + " milliseconds");
            SendResultKey sendResultKey = seqIdToSendCFMap.get(entry.getKey());
            if (sendResultKey != null && item.getContext().getChannel() != null) {
              sendResultKey.cf.cancel(true);
            }
            requestFailed(entry.getKey(), ResponseType.TIMEOUT, "request timeout");
          }
        }

        //for(Entry<PSLocation, Long> entry : psLocToLastChannelTsMap.entrySet()) {
        //  if(ts - entry.getValue() > requestTimeOut * 2)  {
        //    LOG.error("Can not get channel for PS " + entry.getKey() + " over " + (ts - entry.getValue())
        //      + " milliseconds, close all channels to it");
        //    closeChannels(entry.getKey());
        //    psLocToLastChannelTsMap.put(entry.getKey(), ts);
        //  }
        //}
      } catch (Exception x) {
        LOG.error("remove request failed ", x);
      }
    }

    /**
     * if a channel is closed, all request in this channel should be remove and reschedule
     *
     * @param channel closed channel
     */
    private void removeRequestForChannel(Channel channel) {
      LOG.info("remove channel " + channel);
      int removeNum = 0;
      try {
        for (Entry<Integer, Request> entry : seqIdToRequestMap.entrySet()) {
          NettyChannel ch = entry.getValue().getContext().getChannel();
          if (ch != null && ch.getChannel().equals(channel)) {
            removeNum++;
            requestFailed(entry.getKey(), ResponseType.NETWORK_ERROR, "channel is closed");
          }
        }

        LOG.info("remove channel " + channel + ", removeNum=" + removeNum);

        InetSocketAddress address = (InetSocketAddress) (channel.remoteAddress());
        if (address == null) {
          LOG.warn("channel " + channel + " remote address is null");
          return;
        }
        Location loc = new Location(address.getHostName(), address.getPort() - 1);
        ParameterServerId psId = PSAgentContext.get().getLocationManager().getPsId(loc);
        if (psId != null) {
          getChannelContext(new PSLocation(psId, loc)).channelNotactive();
        }

      } catch (Exception x) {
        if (!stopped.get()) {
          LOG.error("remove request failed ", x);
        }
      }
    }
  }

  private void requestSuccess(int seqId) {
    Request request = seqIdToRequestMap.remove(seqId);
    if (request == null) {
      return;
    }

    rpcContext.after(request.getContext().getServerId());
    seqIdToSendCFMap.remove(seqId);
    returnChannel(request);
    switch (request.getType()) {
      case GET_PART:
      case GET_ROWSPLIT:
      case GET_ROWSSPLIT:
      case GET_PSF:
      case INDEX_GET_ROW:
      case INDEX_GET_ROWS:
        getRequestSuccess(request);
        break;

      case PUT_PART:
      case UPDATE:
      case UPDATE_PSF:
        putRequestSuccess(request);
        break;

      default:
        LOG.error("unvalid response for request " + request + " with seqId " + seqId);
        break;
    }
  }

  private void requestFailed(int seqId, ResponseType failedType, String errorLog) {
    Request request = seqIdToRequestMap.remove(seqId);
    if (request == null) {
      return;
    }

    // Check need retry or not
    if (isOverTryLimit(request, failedType, errorLog)) {
      LOG.error("request " + request + " failed over " + maxTryNum + ", notify to caller");
      FutureResult response = requestToResultMap.remove(request);
      if(response != null) {
        response.setExecuteError(errorLog);
      } else {
        LOG.warn("can not find response for request " + request);
      }

      // Clear the cache for this request
      PSAgentContext.get().getUserRequestAdapter().clear(request.getUserRequestId());
      return;
    }

    rpcContext.after(request.getContext().getServerId());
    seqIdToSendCFMap.remove(seqId);
    //LOG.debug("request failed " + request + ", failedType=" + failedType + ", errorLog=" + errorLog);
    returnChannel(request);
    returnBuffer(request);
    resetContext(request);

    switch (request.getType()) {
      case GET_PART:
      case GET_ROWSPLIT:
      case GET_ROWSSPLIT:
      case GET_PSF:
      case INDEX_GET_ROW:
      case INDEX_GET_ROWS:
        getRequestFailed(request, failedType, errorLog);
        break;

      case PUT_PART:
      case UPDATE:
      case UPDATE_PSF:
      case CHECKPOINT:
        putRequestFailed(request, failedType, errorLog);
        break;

      default:
        LOG.error("unvalid request " + request);
        break;
    }
  }

  private void resetContext(Request request) {
    request.getContext().reset();
  }

  private void psFailed(PSLocation psLoc) {
    // Remove all pending RPCS
    removePendingRPCS(psLoc);

    // Close all channel to this PS
    closeChannels(psLoc);
  }

  private void psNormal(PSLocation psLoc) {

  }

  private void removePendingRPCS(PSLocation psLoc) {
    Iterator<Entry<Integer, SendResultKey>> iter = seqIdToSendCFMap.entrySet().iterator();
    Entry<Integer, SendResultKey> entry;
    while (iter.hasNext()) {
      entry = iter.next();
      if (entry.getValue().psLoc.equals(psLoc)) {
        SendResultKey sendResultKey = seqIdToSendCFMap.get(entry.getKey());
        Request request = seqIdToRequestMap.get(entry.getKey());
        if (request != null && request.getContext().getChannel() != null) {
          sendResultKey.cf.cancel(true);
        }
        requestFailed(entry.getKey(), ResponseType.NETWORK_ERROR, "Server no response");
      }
    }
  }

  private void closeChannels(PSLocation psLoc) {
    channelManager.removeChannels(new Location(psLoc.loc.getIp(), psLoc.loc.getPort() + 1));
  }

  private void putRequestSuccess(Request request) {
    try {
      dispatchMessageQueue.put(new RequestDispatchEvent(EventType.PUT_SUCCESS, request));
    } catch (Exception e) {
      LOG.error("add PUT_SUCCESS event for request " + request + " failed, ", e);
    }
  }

  private void getRequestSuccess(Request request) {
    try {
      dispatchMessageQueue.put(new RequestDispatchEvent(EventType.GET_SUCCESS, request));
    } catch (Exception e) {
      LOG.error("add GET_SUCCESS event for request " + request + " failed, ", e);
    }
  }

  private void getRequestNotReady(Request request) {
    try {
      dispatchMessageQueue.put(new RequestDispatchEvent(EventType.GET_NOTREADY, request));
    } catch (Exception e) {
      LOG.error("add GET_NOTREADY event for request " + request + " failed, ", e);
    }
  }

  private void getRequestFailed(Request request, ResponseType failedType, String errorLog) {
    try {
      dispatchMessageQueue
          .put(new RequestFailedEvent(EventType.GET_FAILED, request, failedType, errorLog));
    } catch (Exception e) {
      LOG.error("add GET_FAILED event for request " + request + " failed, ", e);
    }
  }

  private void putRequestFailed(Request request, ResponseType failedType, String errorLog) {
    try {
      dispatchMessageQueue
          .put(new RequestFailedEvent(EventType.PUT_FAILED, request, failedType, errorLog));
    } catch (Exception e) {
      LOG.error("add PUT_FAILED event for request " + request + " failed, ", e);
    }
  }

  private boolean isOverTryLimit(Request request, ResponseType failedType, String errorLog) {
    request.getContext().setErrorLog(errorLog);
    return request.getContext().getTryCounter() >= maxTryNum;
  }

  private void refreshServerLocationSuccess(ParameterServerId serverId, boolean isUpdated) {
    try {
      dispatchMessageQueue.put(
          new RefreshServerLocationEvent(EventType.REFRESH_SERVER_LOCATION_SUCCESS, serverId,
              isUpdated));
    } catch (Exception e) {
      LOG.error("add REFRESH_SERVER_LOCATION_SUCCESS event failed, ", e);
    }
  }

  private void refreshServerLocationFailed(ParameterServerId serverId) {
    try {
      dispatchMessageQueue
          .put(new RefreshServerLocationEvent(EventType.REFRESH_SERVER_LOCATION_FAILED, serverId));
    } catch (Exception e) {
      LOG.error("add REFRESH_SERVER_LOCATION_FAILED event failed, ", e);
    }
  }

  private void startGet() {
    try {
      dispatchMessageQueue.put(new DispatcherEvent(EventType.START_GET));
    } catch (Exception e) {
      LOG.error("add START_GET event failed, ", e);
    }

  }

  private void startPut() {
    try {
      dispatchMessageQueue.put(new DispatcherEvent(EventType.START_PUT));
    } catch (Exception e) {
      LOG.error("add START_PUT event failed, ", e);
    }
  }

  private void returnBuffer(Request item) {
    ByteBuf buf = item.getContext().getSerializedData();
    if (buf != null && buf.refCnt() > 0) {
      try {
        buf.release();
      } catch (Throwable x) {
        LOG.error("Release Buffer failed.", x);
      }
      item.getContext().setSerializedData(null);
    }
  }

  private void returnChannel(Request item) {
    try {
      if (item.getContext().getChannel() != null) {
        channelManager.releaseChannel(item.getContext().getChannel());
        item.getContext().setChannel(null);
      }
    } catch (Exception x) {
      LOG.error("return channel to channel pool failed ", x);
    }
  }

  /**
   * get location of a server from master, it will wait until the location is ready
   *
   * @param serverId server id
   * @return location of the server
   */
  private Location getPSLocFromMaster(ParameterServerId serverId) {
    Location location;
    try {
      location = PSAgentContext.get().getMasterClient().getPSLocation(serverId);
    } catch (ServiceException e) {
      LOG.error("get ps location from master failed.", e);
      return null;
    }

    if (location != null) {
      PSAgentContext.get().getLocationManager().setPsLocation(serverId, location);
    }
    return location;
  }

  public ArrayList<Long> getGetUseTimes() {
    return getUseTimes;
  }

  class SendResultKey {

    public final PSLocation psLoc;
    public final ChannelFuture cf;

    public SendResultKey(PSLocation psLoc, ChannelFuture cf) {
      this.psLoc = psLoc;
      this.cf = cf;
    }
  }


  class GetChannelContext {

    public int failedCounter = 0;
    public int successCounter = 0;
    public int notactiveCounter = 0;
    public long lastCheckTs = System.currentTimeMillis();
    public final ReentrantLock lock = new ReentrantLock();

    public void getChannelSuccess() {
      try {
        lock.lock();
        successCounter++;
      } finally {
        lock.unlock();
      }
    }

    public void getChannelFailed() {
      try {
        lock.lock();
        failedCounter++;
      } finally {
        lock.unlock();
      }
    }

    public void channelNotactive() {
      try {
        lock.lock();
        notactiveCounter++;
      } finally {
        lock.unlock();
      }
    }

    public void reset() {
      try {
        lock.lock();
        failedCounter = 0;
        successCounter = 0;
        notactiveCounter = 0;
        lastCheckTs = System.currentTimeMillis();
      } finally {
        lock.unlock();
      }
    }

    public int getFailedCounter() {
      try {
        lock.lock();
        return failedCounter;
      } finally {
        lock.unlock();
      }
    }

    public int getSuccessCounter() {
      try {
        lock.lock();
        return successCounter;
      } finally {
        lock.unlock();
      }
    }

    public int getInactiveCounter() {
      try {
        lock.lock();
        return notactiveCounter;
      } finally {
        lock.unlock();
      }
    }

    public long getLastCheckTs() {
      try {
        lock.lock();
        return lastCheckTs;
      } finally {
        lock.unlock();
      }
    }
  }

  private GetChannelContext getChannelContext(PSLocation psLoc) {
    GetChannelContext context = psLocToGetChannelContextMap.get(psLoc);
    if (context == null) {
      context = psLocToGetChannelContextMap.putIfAbsent(psLoc, new GetChannelContext());
      if (context == null) {
        context = psLocToGetChannelContextMap.get(psLoc);
      }
    }
    return context;
  }

  class GrayServer {

    public final PSLocation psLoc;
    public final long ts;
    public volatile long lastHBTs;
    public volatile ServerState state;

    public GrayServer(PSLocation psLoc, long ts) {
      this.psLoc = psLoc;
      this.ts = ts;
      this.lastHBTs = ts;
    }
  }


  /**
   * The RPC request sender.
   */
  class Requester extends Thread {

    private final Request request;
    private int seqId;

    public Requester(Request request, int seqId) {
      this.seqId = seqId;
      this.request = request;
    }

    @Override
    public void run() {
      try {
        sendRequest(seqId, request);
      } catch (InterruptedException e) {
        LOG.error("send request " + request + " is interrupted");
      } catch (Throwable e) {
        LOG.error("send request " + request + " failed, ", e);
      }
    }

    /**
     * build the request and serialize it, then send it to server
     *
     * @param seqId request id
     * @param request request context
     */
    private void sendRequest(int seqId, Request request) throws InterruptedException {
      long startTs = System.currentTimeMillis();

      // Get server id and location for this request
      PSLocation psLoc = getPSLoc(request);
      request.getContext().setActualServerId(psLoc.psId);
      request.getContext().setLocation(psLoc.loc);
      request.getContext().addTryCounter();

      // If location is null, means that the server is not ready
      if (psLoc.loc == null) {
        LOG.error("request " + request + " server " + request.getContext().getServerId()
            + " location is null");
        if (psLoc.loc == null) {
          requestFailed(seqId, ResponseType.SERVER_NOT_READY, "location is null");
          return;
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("request " + request + " with seqId=" + seqId + " get location use time " + (
            System.currentTimeMillis() - startTs));
      }

      // Get the channel for the location
      startTs = System.currentTimeMillis();
      Channel channel;
      try {
        channel = getChannel(request, psLoc).getChannel();
      } catch (Throwable e) {
        if (!stopped.get()) {
          getChannelContext(psLoc).getChannelFailed();
          LOG.error("get channel for " + psLoc.loc + " failed ", e);
          requestFailed(seqId, ResponseType.NETWORK_ERROR, StringUtils.stringifyException(e));
        }
        return;
      }

      getChannelContext(psLoc).getChannelSuccess();

      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "request with seqId=" + seqId + " get channel use time " + (System.currentTimeMillis()
                - startTs));
      }

      // Check if need get token first
      int token;

      if (psIdToStateMap.get(psLoc.psId) == ServerState.BUSY) {
        requestFailed(seqId, ResponseType.SERVER_IS_BUSY, "server " + psLoc.psId + " is busy");
        return;
      } else if (psIdToStateMap.get(psLoc.psId) == ServerState.GENERAL) {
        try {
          LOG.debug("PS " + psLoc + " is in GENERAL, need get token first");
          token = getToken(psLoc.loc);
          LOG.debug("token=" + token);
          if (token == 0) {
            requestFailed(seqId, ResponseType.SERVER_IS_BUSY, "PS " + psLoc.psId + " is busy");
            return;
          }
          request.setTokenNum(token);
        } catch (Throwable e) {
          LOG.error("get token from PS " + psLoc.loc + " failed ", e);
          requestFailed(seqId, ResponseType.NETWORK_ERROR, StringUtils.stringifyException(e));
          return;
        }
      }

      LOG.info("Send request " + request.getHeader());

      // Serialize the request
      startTs = System.currentTimeMillis();
      ByteBuf buffer;
      try {
        buffer = serializeRequest(request);
      } catch (Throwable e) {
        if (e instanceof OutOfMemoryError) {
          rpcContext.oom();
        }

        LOG.error("serialize request " + request + " failed ", e);
        requestFailed(seqId, ResponseType.OOM, StringUtils.stringifyException(e));
        return;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("request with seqId=" + seqId + " serialize request use time " + (
            System.currentTimeMillis() - startTs));
      }

      // Send the request
      request.getContext().setSendStartTs(System.currentTimeMillis());
      ChannelFuture cf = channel.writeAndFlush(buffer);
      cf.addListener(new RequesterChannelFutureListener(seqId, request));

      seqIdToSendCFMap.put(seqId, new SendResultKey(
          new PSLocation(request.getContext().getActualServerId(),
              request.getContext().getLocation()), cf));
    }

    private int getToken(Location loc) throws IOException, ServiceException {
      return PSAgentContext.get().getPSControlClientManager().getOrCreatePSClient(loc).getToken(0);
    }

    private PSLocation getPSLoc(Request request) {
      ParameterServerId serverId = null;
      Location loc = null;
      PartitionLocation partLoc;
      try {
        partLoc = PSAgentContext.get().getMatrixMetaManager()
            .getPartLocation(request.getHeader().matrixId, request.getHeader().partId, disableRouterCache);
      } catch (Throwable e) {
        LOG.error("Get partition location from Master failed ", e);
        partLoc = PSAgentContext.get().getMatrixMetaManager()
            .getPartLocation(request.getHeader().matrixId, request.getHeader().partId);
      }

      if (partLoc != null && !partLoc.psLocs.isEmpty()) {
        serverId = partLoc.psLocs.get(0).psId;
        loc = partLoc.psLocs.get(0).loc;
      }

      if (loc == null && !disableRouterCache) {
        try {
          partLoc = PSAgentContext.get().getMatrixMetaManager()
              .getPartLocation(request.getHeader().matrixId, request.getHeader().partId, true);
        } catch (Throwable e) {
          LOG.error("Get partition location from Master failed ", e);
        }

        if (partLoc != null && !partLoc.psLocs.isEmpty()) {
          serverId = partLoc.psLocs.get(0).psId;
          loc = partLoc.psLocs.get(0).loc;
        }
      }

      return new PSLocation(serverId, loc);
    }

    private NettyChannel getChannel(Request request, PSLocation psLoc) throws Exception {
      // get a channel to server from pool
      long startTs = System.currentTimeMillis();
      NettyChannel channel =
          channelManager.getChannel(new Location(psLoc.loc.getIp(), psLoc.loc.getPort() + 1));
      if (LOG.isDebugEnabled()) {
        LOG.debug("request with seqId=" + seqId + " wait for channel use time " + (
            System.currentTimeMillis() - startTs));
      }

      // if channel is not valid, it means maybe the connections to the server are closed
      if (!channel.getChannel().isActive() || !channel.getChannel().isOpen()) {
        getChannelContext(psLoc).channelNotactive();
        LOG.error("channel " + channel + " is not active");
        throw new IOException("channel " + channel + " is not active");
      }

      //request.getContext().setChannelPool(pool);
      request.getContext().setChannel(channel);
      return channel;
    }
  }

  private ByteBuf serializeRequest(Request request) {
    // Allocate the bytebuf and serialize the request
    ByteBuf buffer = ByteBufUtils.newByteBuf(request.bufferLen());
    request.serialize(buffer);
    return buffer;
  }

  private String poolInfo(GenericObjectPool<Channel> pool) {
    StringBuilder sb = new StringBuilder();
    sb.append("active connects=");
    sb.append(pool.getNumActive());
    sb.append("\t");
    sb.append("active connects=");
    sb.append(pool.getNumIdle());
    return sb.toString();
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
      LOG.debug("send request with seqId=" + seqId + " complete");
      if (!future.isSuccess()) {
        LOG.error("send request " + seqId + " failed ", future.cause());
        requestFailed(seqId, ResponseType.NETWORK_ERROR,
            "send request failed " + future.cause().toString());
      } else {
        returnChannel(request);
      }
    }
  }


  public class ResponseDispatcher extends Thread {

    @Override
    public void run() {
      try {
        while (!stopped.get() && !Thread.interrupted()) {
          responseThreadPool.execute(new Responser(msgQueue.take()));
        }
      } catch (InterruptedException e) {
        if (!stopped.get()) {
          LOG.fatal("ResponseDispatcher is interrupted!! ", e);
          PSAgentContext.get().getPsAgent().error("ResponseDispatcher is interrupted!!");
        }
      } catch (Throwable e) {
        LOG.fatal("ResponseDispatcher is failed ", e);
        PSAgentContext.get().getPsAgent().error("ResponseDispatcher is failed. " + e.getMessage());
      }
    }
  }

  public void handleResponse(Object msg) {
    responseThreadPool.execute(new Responser((ByteBuf) msg));
  }

  /**
   * RPC responses handler
   */
  public class Responser extends Thread {

    private ByteBuf msg;

    public Responser(ByteBuf msg) {
      this.msg = msg;
    }

    @Override
    public void run() {
      int seqId = 0;
      try {
        // Parse response
        Response response = new Response();
        response.deserialize(msg);
        ResponseHeader header = response.getHeader();
        seqId = header.getSeqId();

        LOG.info("ResponseHeader=" + header);

        // Get Request
        Request request = seqIdToRequestMap.get(header.seqId);
        if(request == null) {
          return;
        }

        // TODO: for LDA, will be remove future
        FutureResult subResult = requestToResultMap.get(request);
        if(request.getType() == TransportMethod.GET_PSF && subResult != null) {
          GetUDFResponse getUDFResponse = new GetUDFResponse();
          getUDFResponse.deserialize(msg);
          subResult.set(getUDFResponse.getPartResult());
        }

        if(request.getType() == TransportMethod.UPDATE_PSF && subResult != null) {
          UpdateUDFResponse updateUDFResponse = new UpdateUDFResponse();
          updateUDFResponse.deserialize(msg);
          subResult.set(new VoidResult(com.tencent.angel.psagent.matrix.ResponseType.SUCCESS));
        }

        int userRequestId = request.getUserRequestId();

        // Update Server state
        handleServerState(request, header.state);

        //LOG.info("Handle request " + request.getHeader() + ", response " + response.getHeader());

        // Get user request and result cache
        UserRequestAdapter adapter = PSAgentContext.get().getUserRequestAdapter();
        UserRequest userRequest = adapter.getUserRequest(userRequestId);
        ResponseCache responseCache = adapter.getResponseCache(userRequestId);
        FutureResult futureResult = adapter.getFutureResult(userRequestId);

        //LOG.info("userRequest=" + userRequest + ", responseCache=" + responseCache + ", futureResult=" + futureResult);

        if(userRequest == null || responseCache == null || futureResult == null) {
          // Some error happens, just return
          return;
        }

        //LOG.info("responseCache " + responseCache.getExpectedResponseNum());

        switch (header.getResponseType()) {
          case SUCCESS: {
            // Get response handler
            TransportMethod method = TransportMethod.valueOf(header.methodId);
            Handler responseHandler = responseHanders.get(method);
            if(responseHandler == null) {
              LOG.error("Can not find handler for method " + method);
              return;
            }

            // Add the response to the cache
            responseCache.add(request, response);

            // Check can merge or not
            if(responseCache.canMerge()) {
              // Merge
              responseHandler.handle(futureResult, userRequest, responseCache);

              // Clear response cache
              responseCache.clear();

              // Remove the response cache
              adapter.clear(userRequestId);
            }

            // Handle success
            requestSuccess(seqId);
            break;
          }

          case OOM:
          case SERVER_IS_BUSY:
          case SERIALIZE_RESPONSE_FAILED: {
            // Server is busy now, retry
            requestFailed(seqId, ResponseType.SERVER_IS_BUSY, header.detail);
            break;
          }

          case SERVER_NOT_READY: {
            // Server is not ready, retry
            requestFailed(seqId, ResponseType.SERVER_NOT_READY, header.detail);
            break;
          }

          case PARSE_HEADER_FAILED:
          case UNSUPPORT_REQUEST:
          case SERVER_HANDLE_FAILED:
          case SERVER_HANDLE_FATAL: {
            // Handle failed, just return error
            futureResult.setExecuteException(new ExecutionException(
                new AngelException("Handle request failed " + header.detail)));
            adapter.clear(request.getUserRequestId());
            break;
          }
        }
      } catch (Throwable x) {
        LOG.error("hanlder rpc response failed ", x);
        if (x instanceof OutOfMemoryError) {
          // Parse response msg failed
          rpcContext.oom();
          requestFailed(seqId, ResponseType.OOM, StringUtils.stringifyException(x));
        } else {
          requestFailed(seqId, ResponseType.UNKNOWN_ERROR, StringUtils.stringifyException(x));
        }
      } finally {
        msg.release();
      }
    }

    private void handleServerState(Request request, ServerState state) {
      if (state != null) {
        psIdToStateMap.put(request.getContext().getActualServerId(), state);
      }
    }
  }
}
