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

package com.tencent.angel.psagent.matrix.transport;

import com.google.protobuf.ServiceException;
import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.common.transport.ChannelManager;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.matrix.PartitionLocation;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ml.matrix.psf.update.enhance.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.enhance.VoidResult;
import com.tencent.angel.ml.matrix.transport.*;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;
import com.tencent.angel.utils.ByteBufUtils;
import com.tencent.angel.utils.StringUtils;
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

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * RPC client to parameter servers. It uses Netty as the network communication framework.
 */
public class MatrixTransportClient implements MatrixTransportInterface {
  private static final Log LOG = LogFactory.getLog(MatrixTransportClient.class);

  /** partition request id to request context map */
  private final ConcurrentHashMap<Integer, Request> seqIdToRequestMap;

  /** request to result map */
  @SuppressWarnings("rawtypes")
  private final ConcurrentHashMap<Request, FutureResult> requestToResultMap;

  /** netty client bootstrap */
  private Bootstrap bootstrap;

  /** netty client thread pool */
  private EventLoopGroup eventGroup;

  /** un-decoded result queue */
  private final LinkedBlockingQueue<ByteBuf> msgQueue;

  /** stop the all services in the rpc client */
  private final AtomicBoolean stopped;

  /** event dispatcher for matrix transport client */
  private RequestDispatcher requestDispacher;

  /** event queue */
  private final LinkedBlockingQueue<DispatcherEvent> dispatchMessageQueue;

  /** ps id to partition get requests map */
  private final ConcurrentHashMap<ParameterServerId, LinkedBlockingQueue<Request>> getItemQueues;

  /** ps id to partition put requests map */
  private final ConcurrentHashMap<ParameterServerId, LinkedBlockingQueue<Request>> putItemQueues;

  /**
   * timed event generator, it used to check there are failed partition requests we should
   * re-dispatch periodically
   */
  private final Timer timer;

  /** timed event generation interval in milliseconds */
  private final int checkPeriodMS;

  /** retry interval in milliseconds for failed requests */
  private final int retryIntervalMs;

  /**
   * client worker pool: 1.use to deserialize partition responses and merge them to final result
   * 2.use to generate partition request and serialize it
   * */
  private ExecutorService requestThreadPool;

  /**
   * client worker pool: 1.use to deserialize partition responses and merge them to final result
   * 2.use to generate partition request and serialize it
   * */
  private ExecutorService hbThreadPool;

  /**
   * client worker pool: 1.use to deserialize partition responses and merge them to final result
   * 2.use to generate partition request and serialize it
   * */
  private ExecutorService responseThreadPool;

  /** response message handler */
  private ResponseDispatcher responseDispatcher;

  private final ArrayList<Long> getUseTimes;

  /** channel pool manager:it maintain a channel pool for every server */
  private ChannelManager channelManager;

  /** use direct netty buffer or not */
  private final boolean useDirectBuffer;

  /** use pooled netty buffer or not */
  private final boolean usePool;

  private final boolean disableRouterCache;

  private final int partReplicNum;

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
  private final ConcurrentHashMap<ParameterServerId, GrayServer> grayServers = new ConcurrentHashMap<>();

  /**
   * Failed PS to old loc map
   */
  private final ConcurrentHashMap<ParameterServerId, Location> failedPSToLocMap = new ConcurrentHashMap<>();

  /**
   * Request id to request send result map
   */
  private final ConcurrentHashMap<Integer, SendResultKey> seqIdToSendCFMap = new ConcurrentHashMap<>();

  /**
   * Request id generator
   */
  private final AtomicInteger currentSeqId = new AtomicInteger(0);

  /**
   * PS id to PS state map
   */
  private final ConcurrentHashMap<ParameterServerId, ServerState> psIdToStateMap = new ConcurrentHashMap<>();

  /**
   * RPC running context
   */
  private final RPCContext rpcContext;

  /**
   * PS location to last get channel timestamp map
   */
  private final ConcurrentHashMap<PSLocation, GetChannelContext> psLocToGetChannelContextMap = new ConcurrentHashMap<>();

  /**
   * PS location to "no active channel" error counter map
   */
  private final ConcurrentHashMap<PSLocation, AtomicInteger> psLocToNoActiveCounterMap = new ConcurrentHashMap<>();

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

    getUseTimes = new ArrayList<>();

    msgQueue = new LinkedBlockingQueue<>();
    stopped = new AtomicBoolean(false);

    Configuration conf = PSAgentContext.get().getConf();
    timer = new Timer();
    checkPeriodMS =
        conf.getInt(AngelConf.ANGEL_MATRIXTRANSFER_CHECK_INTERVAL_MS,
            AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_CHECK_INTERVAL_MS);

    retryIntervalMs =
        conf.getInt(AngelConf.ANGEL_MATRIXTRANSFER_RETRY_INTERVAL_MS,
            AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_RETRY_INTERVAL_MS);

    useDirectBuffer =
        conf.getBoolean(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_USEDIRECTBUFFER,
            AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_USEDIRECTBUFFER);

    usePool =
        conf.getBoolean(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_USEPOOL,
            AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_USEPOOL);

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

    int nettyWorkerNum =
        conf.getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_EVENTGROUP_THREADNUM,
            AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_EVENTGROUP_THREADNUM);

    int sendBuffSize =
        conf.getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_SNDBUF,
            AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_SNDBUF);

    int recvBuffSize =
        conf.getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_RCVBUF,
            AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_RCVBUF);
    
    final int maxMessageSize = 
        conf.getInt(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_MAX_MESSAGE_SIZE,
            AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_MAX_MESSAGE_SIZE);

    requestThreadPool = Executors.newFixedThreadPool(
        conf.getInt(AngelConf.ANGEL_MATRIXTRANSFER_CLIENT_REQUESTER_POOL_SIZE,
            AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_CLIENT_REQUESTER_POOL_SIZE));

    responseThreadPool = Executors.newFixedThreadPool(
      conf.getInt(AngelConf.ANGEL_MATRIXTRANSFER_CLIENT_RESPONSER_POOL_SIZE,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_CLIENT_RESPONSER_POOL_SIZE));

    hbThreadPool = Executors.newFixedThreadPool(8);

    bootstrap = new Bootstrap();
    channelManager = new ChannelManager(bootstrap);

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

    eventGroup = new NioEventLoopGroup(nettyWorkerNum);
    ((NioEventLoopGroup)eventGroup).setIoRatio(70);

    bootstrap.group(eventGroup).channel(NioSocketChannel.class)
        .option(ChannelOption.SO_SNDBUF, sendBuffSize)
        .option(ChannelOption.SO_RCVBUF, recvBuffSize)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeLine = ch.pipeline();
            pipeLine.addLast(new LengthFieldBasedFrameDecoder(maxMessageSize, 0, 4, 0, 4));
            pipeLine.addLast(new LengthFieldPrepender(4));
            pipeLine.addLast(new MatrixTransportClientHandler(msgQueue, dispatchMessageQueue, rpcContext));
          }
        });
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
      } catch (InterruptedException e) {
        LOG.error("Timer interrupted");
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
        channelManager.clear();
      }

      if(eventGroup != null) {
        eventGroup.shutdownGracefully();
      }

      if(requestThreadPool != null) {
        requestThreadPool.shutdownNow();
      }

      if(responseThreadPool != null) {
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

  @Override
  public Future<ServerPartition> getPart(PartitionKey partKey, int clock) {
    ParameterServerId serverId = PSAgentContext.get().getMatrixMetaManager().getMasterPS(partKey);
    GetPartitionRequest request = new GetPartitionRequest(partKey, clock);

    FutureResult<ServerPartition> future = new FutureResult<>();
    requestToResultMap.put(request, future);
    addToGetQueueForServer(serverId, request);
    startGet();
    return future;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Future<ServerRow> getRowSplit(PartitionKey partKey, int rowIndex, int clock) {
    ParameterServerId serverId = PSAgentContext.get().getMatrixMetaManager().getMasterPS(partKey);
    GetRowSplitRequest request = new GetRowSplitRequest(clock, partKey, rowIndex);
    FutureResult<ServerRow> future = new FutureResult<>();
    FutureResult<ServerRow> oldFuture = requestToResultMap.putIfAbsent(request, future);
    if (oldFuture != null) {
      LOG.debug("same request exist, just return old future");
      return oldFuture;
    } else {
      addToGetQueueForServer(serverId, request);
      startGet();
      return future;
    }
  }

  @Override
  public Future<List<ServerRow>> getRowsSplit(PartitionKey partKey, List<Integer> rowIndexes,
      int clock) {
    ParameterServerId serverId = PSAgentContext.get().getMatrixMetaManager().getMasterPS(partKey);
    GetRowsSplitRequest request = new GetRowsSplitRequest(clock, partKey, rowIndexes);
    FutureResult<List<ServerRow>> future = new FutureResult<>();
    requestToResultMap.put(request, future);
    addToGetQueueForServer(serverId, request);
    startGet();
    return future;
  }

  @Override
  public Future<VoidResult> putPart(PartitionKey partKey, List<RowUpdateSplit> rowsSplit,
      int taskIndex, int clock, boolean updateClock) {
    ParameterServerId serverId = PSAgentContext.get().getMatrixMetaManager().getMasterPS(partKey);
    PutPartitionUpdateRequest request =
        new PutPartitionUpdateRequest(taskIndex, clock, partKey, rowsSplit, updateClock);

    FutureResult<VoidResult> future = new FutureResult<>();
    requestToResultMap.put(request, future);
    addToPutQueueForServer(serverId, request);
    startPut();
    return future;
  }

  @Override
  public Future<GetClocksResponse> getClocks(ParameterServerId serverId) {
    GetClocksRequest request = new GetClocksRequest(serverId);
    FutureResult<GetClocksResponse> future = new FutureResult<>();
    requestToResultMap.put(request, future);
    addToGetQueueForServer(serverId, request);
    startGet();
    return future;
  }

  private void addToGetQueueForServer(ParameterServerId serverId, Request request) {
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
    request.getContext().setServerId(serverId);
    LinkedBlockingQueue<Request> queue = putItemQueues.get(serverId);
    if (queue == null) {
      queue = new LinkedBlockingQueue<>();
      putItemQueues.putIfAbsent(serverId, queue);
      queue = putItemQueues.get(serverId);
    }
    queue.add(request);
  }

  class PSLocRefresher extends Thread {
    @Override
    public void run() {
      while(!stopped.get() && !Thread.interrupted()) {
        Iterator<Entry<ParameterServerId, Location>> iter = failedPSToLocMap.entrySet().iterator();
        while(iter.hasNext()) {
          Entry<ParameterServerId, Location> entry = iter.next();
          try {
            Location loc = PSAgentContext.get().getMasterClient().getPSLocation(entry.getKey());
            if(loc != null && !loc.equals(entry.getValue())) {
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
        try {
          Thread.sleep(5000);
        } catch (Throwable e) {
          if(!stopped.get()) {
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
      psHbTimeIntervalMS =
        conf.getInt(AngelConf.ANGEL_PSAGENT_TO_PS_HEARTBEAT_INTERVAL_MS,
          AngelConf.DEFAULT_ANGEL_PSAGENT_TO_PS_HEARTBEAT_INTERVAL_MS);

      psHbTimeOutMS =
        conf.getInt(AngelConf.ANGEL_PSAGENT_TO_PS_HEARTBEAT_TIMEOUT_MS,
          AngelConf.DEFAULT_ANGEL_PSAGENT_TO_PS_HEARTBEAT_TIMEOUT_MS);
    }

    @Override
    public void run() {
      while(!stopped.get() && !Thread.interrupted()) {
        Iterator<Entry<ParameterServerId, GrayServer>> iter = grayServers.entrySet().iterator();
        Entry<ParameterServerId, GrayServer> entry;
        while(iter.hasNext()) {
          entry = iter.next();
          LOG.warn("PS " + entry.getKey() + " is in gray server list, Send hb to it to check");

          // Check PS exist or not
          if(isPSExited(entry.getValue())) {
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
          if(isTimeOut(entry.getValue())) {
            LOG.error("PS " + entry.getValue().psLoc + " heartbeat timeout, notify to Master");
            try {
              notifyServerFailed(entry.getValue(), true);
            } catch (ServiceException e) {
              LOG.error("Notify PS " + entry.getValue().psLoc + " failed message to Master failed ", e);
              continue;
            }
            iter.remove();
            continue;
          }

          if(shouldRemoveFromGrayList(entry.getValue())) {
            LOG.debug("PS " + entry.getValue().psLoc + " state back to normal, remove it from gray server list");
            iter.remove();
            notifyServerNormal(entry.getValue());
            continue;
          }
          sendHeartBeat(entry.getValue().psLoc);
        }
        try {
          Thread.sleep(psHbTimeIntervalMS);
        } catch (Throwable e) {
          if(!stopped.get()) {
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
      return server.state != null && (server.state == ServerState.IDLE || server.state == ServerState.GENERAL);
    }
  }

  private void notifyServerFailed(GrayServer server, boolean notifyToMaster) throws ServiceException {
    try {
      dispatchMessageQueue.put(new ServerEvent(EventType.SERVER_FAILED, server.psLoc));
    } catch (Exception e) {
      LOG.error("add SERVER_FAILED event for request failed, ", e);
    }

    if(notifyToMaster) {
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

  private void sendHeartBeat(final PSLocation psLoc) {
    hbThreadPool.execute(() -> {
      try {
        LOG.debug("Start to send hb to " + psLoc);
        ServerState state = PSAgentContext.get().getPSControlClientManager().getOrCreatePSClient(psLoc.loc).getState();
        GrayServer server = grayServers.get(psLoc.psId);
        if(server != null) {
          server.state = state;
          server.lastHBTs = System.currentTimeMillis();
        }
      } catch (Throwable e) {
        LOG.error("Send heartbeat to " + psLoc + " failed ", e);

        // Check PS restart or not
        try {
          if(PSAgentContext.get().getMasterClient().isPSExited(psLoc)) {
            GrayServer server = grayServers.get(psLoc.psId);
            if(server != null) {
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

    /** server ids and last choose server indexes */
    private final ParameterServerId[] psIds;

    /** the index of server that last schedule put request send to */
    private int lastChosenPutServerIndex;

    /** the index of server that last schedule get request send to */
    public int lastChosenGetServerIndex;

    /** refreshing server set:the partition request for refreshing server can not be scheduled */
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

      requestTimeOut =
          conf.getInt(AngelConf.ANGEL_MATRIXTRANSFER_REQUEST_TIMEOUT_MS,
              AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_REQUEST_TIMEOUT_MS);

      psHbTimeOutMS =
        conf.getInt(AngelConf.ANGEL_PSAGENT_TO_PS_HEARTBEAT_TIMEOUT_MS,
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
              if(request instanceof PartitionRequest) {
                request.getContext().setNextRetryTs(genNextRetryTs());
                failedGetCache.add(request);
              }

              // Add the server to gray server list
              if(needPutToGrayServers(getFailedEvent.getFailedType())) {
                addToGrayList(new PSLocation(request.getContext().getActualServerId(), request.getContext().getLocation()));
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
              if(request instanceof PartitionRequest) {
                request.getContext().setNextRetryTs(genNextRetryTs());
                failedPutCache.add(request);
              }

              // Add the server to gray server list
              if(needPutToGrayServers(putFailedEvent.getFailedType())) {
                addToGrayList(new PSLocation(request.getContext().getActualServerId(), request.getContext().getLocation()));
              }

              putDataSplit();
              break;
            }

            case SERVER_FAILED: {
              ServerEvent serverFailedEvent = (ServerEvent)event;

              // Move from gray server list to failed server list
              failedPSToLocMap.put(serverFailedEvent.getPsLoc().psId, serverFailedEvent.getPsLoc().loc);
              grayServers.remove(serverFailedEvent.getPsLoc().psId);

              // Handle the RPCS to this server
              psFailed(serverFailedEvent.getPsLoc());
              break;
            }

            case SERVER_NORMAL: {
              ServerEvent serverNormalEvent = (ServerEvent)event;
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
          if(!stopped.get()) {
            LOG.fatal("RequestDispatcher is interrupted!! ", e);
            PSAgentContext.get().getPsAgent().error("RequestDispatcher is interrupted!!");
          }
        }
        catch (Throwable e) {
          LOG.fatal("RequestDispatcher is failed ", e);
          PSAgentContext.get().getPsAgent().error("RequestDispatcher is failed. " + e.getMessage());
        }
      }
    }

    private long genNextRetryTs() {
      long startTs = System.currentTimeMillis();
      Random r = new Random();
      return startTs + (long)(requestTimeOut * r.nextDouble());
    }

    private boolean needPutToGrayServers(ResponseType type) {
      return type == ResponseType.NETWORK_ERROR
        || type == ResponseType.SERVER_IS_BUSY
        || type == ResponseType.TIMEOUT
        || type == ResponseType.CONNECT_REFUSED;
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
        if (!isInGrayList(psIds[index]) && !isFailed(psIds[index]) && !checkIsOverReqNumLimit(psIds[index])) {
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
      if(queue.isEmpty()) {
        return 0;
      }

      Request item;
      int submitNum = 0;
      while((item = queue.poll()) != null) {
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
      return ((rpcContext.getInflightRPCCounters() + 1) > rpcContext.getMaxInflightRPCNum())
        || ((rpcContext.getServerInflightRPCCounters(serverId) + 1) > rpcContext.getMaxInflightRPCNumPerServer());
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
      item.getContext().setWaitTimeTicks(0);
      if(item instanceof PartitionRequest) {
        rpcContext.before(item.getContext().getServerId());
      }
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
      for (Entry<ParameterServerId, LinkedBlockingQueue<Request>> entry : getItemQueues.entrySet()) {
        LOG.info("period check, for server " + entry.getKey() + ", there is "
            + entry.getValue().size() + " get items need dispatch ");
      }

      for (Entry<ParameterServerId, LinkedBlockingQueue<Request>> entry : putItemQueues.entrySet()) {
        LOG.info("period check, for server " + entry.getKey() + ", there is "
            + entry.getValue().size() + " put items need dispatch ");
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
      if(tickClock % 100 == 0) {
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
        if ((serverId == null || requestServerId == serverId)
            && !isInGrayList(requestServerId)
            && !isFailed(requestServerId)
            && (ts - item.getContext().getNextRetryTs() > 0)) {
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
        if ((serverId == null || requestServerId == serverId)
            && !isInGrayList(item.getContext().getServerId())
            && !isFailed(requestServerId)
            && (ts - item.getContext().getNextRetryTs() > 0)) {
            //&& (ts - item.getContext().getFailedTs() >= retryIntervalMs)) {
          schedulableFailedGetCache.add(item);
          iter.remove();
        }
      }
      getDataSplit();
      if (LOG.isDebugEnabled() && (tickClock % 100 == 0)) {
        printDispatchInfo();
      }
    }

    private void checkChannelContext() {
      long startTs = System.currentTimeMillis();
      for(Entry<PSLocation, GetChannelContext> entry : psLocToGetChannelContextMap.entrySet()) {
        if ((entry.getValue().getInactiveCounter() > 0)
          || ((startTs - entry.getValue().getLastCheckTs() > requestTimeOut * 2)
             && (entry.getValue().getSuccessCounter() == 0 && entry.getValue().failedCounter > 0))) {
          LOG.error("Channel for ps " + entry.getKey() + " noactive channel happened time:"
            + entry.getValue().getInactiveCounter() + ", success time:" + entry.getValue().successCounter
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
          if(item.getContext().getSendStartTs() > 0 && ((ts - item.getContext().getSendStartTs()) > requestTimeOut)) {
            LOG.error("Request " + item + " to PS " + item.getContext().getActualServerId()
              + " not return result over " + requestTimeOut + " milliseconds");
            SendResultKey sendResultKey = seqIdToSendCFMap.get(entry.getKey());
            if(sendResultKey != null && item.getContext().getChannel() != null) {
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
          Channel ch = entry.getValue().getContext().getChannel();
          if (ch != null && ch.equals(channel)) {
            removeNum++;
            requestFailed(entry.getKey(), ResponseType.NETWORK_ERROR, "channel is closed");
          }
        }

        LOG.info("remove channel " + channel + ", removeNum=" + removeNum);
      } catch (Exception x) {
        LOG.fatal("remove request failed ", x);
      }
    }
  }

  private void requestSuccess(int seqId) {
    Request request = seqIdToRequestMap.remove(seqId);
    if(request == null) {
      return;
    }

    if(request instanceof PartitionRequest) {
      rpcContext.after(request.getContext().getServerId());
    }
    seqIdToSendCFMap.remove(seqId);
    returnChannel(request);
    switch (request.getType()) {
      case GET_PART:
      case GET_ROWSPLIT:
      case GET_ROWSSPLIT:
      case GET_CLOCKS:
      case GET_UDF:
        getRequestSuccess(request);
        break;

      case PUT_PART:
      case PUT_PARTUPDATE:
      case UPDATER:
        putRequestSuccess(request);
        break;

      default:
        LOG.error("unvalid response for request " + request + " with seqId " + seqId);
        break;
    }
  }

  private void requestFailed(int seqId, ResponseType failedType, String errorLog) {
    Request request = seqIdToRequestMap.remove(seqId);
    if(request == null) {
      return;
    }

    if(request instanceof PartitionRequest) {
      rpcContext.after(request.getContext().getServerId());
    }
    seqIdToSendCFMap.remove(seqId);
    //LOG.debug("request failed " + request + ", failedType=" + failedType + ", errorLog=" + errorLog);
    returnChannel(request);
    returnBuffer(request);
    resetContext(request);

    switch (request.getType()) {
      case GET_PART:
      case GET_ROWSPLIT:
      case GET_ROWSSPLIT:
      case GET_UDF:
        getRequestFailed(request, failedType, errorLog);
        break;

      case PUT_PART:
      case PUT_PARTUPDATE:
      case UPDATER:
        putRequestFailed(request, failedType, errorLog);
        break;

      case GET_CLOCKS: {
        FutureResult<GetClocksResponse> result = requestToResultMap.remove(request);
        if(result != null) {
          result.set(new GetClocksResponse(failedType, errorLog, null));
        }
        getRequestFailed(request, failedType, errorLog);
        break;
      }

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

  private void  psNormal(PSLocation psLoc) {

  }

  private void removePendingRPCS(PSLocation psLoc) {
    Iterator<Entry<Integer, SendResultKey>> iter = seqIdToSendCFMap.entrySet().iterator();
    Entry<Integer, SendResultKey> entry;
    while(iter.hasNext()) {
      entry = iter.next();
      if(entry.getValue().psLoc.equals(psLoc)) {
        SendResultKey sendResultKey = seqIdToSendCFMap.get(entry.getKey());
        Request request = seqIdToRequestMap.get(entry.getKey());
        if(request != null && request.getContext().getChannel() != null) {
          sendResultKey.cf.cancel(true);
        }
        requestFailed(entry.getKey(), ResponseType.NETWORK_ERROR, "Server no response");
      }
    }
  }

  private void closeChannels(PSLocation psLoc) {
    channelManager.removeChannelPool(new Location(psLoc.loc.getIp(), psLoc.loc.getPort() + 1));
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
      dispatchMessageQueue.put(new RequestFailedEvent(EventType.GET_FAILED, request, failedType, errorLog));
    } catch (Exception e) {
      LOG.error("add GET_FAILED event for request " + request + " failed, ", e);
    }
  }

  private void putRequestFailed(Request request, ResponseType failedType, String errorLog) {
    try {
      dispatchMessageQueue.put(new RequestFailedEvent(EventType.PUT_FAILED, request, failedType, errorLog));
    } catch (Exception e) {
      LOG.error("add PUT_FAILED event for request " + request + " failed, ", e);
    }
  }

  private void refreshServerLocationSuccess(ParameterServerId serverId, boolean isUpdated) {
    try {
      dispatchMessageQueue.put(new RefreshServerLocationEvent(
        EventType.REFRESH_SERVER_LOCATION_SUCCESS, serverId, isUpdated));
    } catch (Exception e) {
      LOG.error("add REFRESH_SERVER_LOCATION_SUCCESS event failed, ", e);
    }
  }

  private void refreshServerLocationFailed(ParameterServerId serverId) {
    try {
      dispatchMessageQueue.put(new RefreshServerLocationEvent(
        EventType.REFRESH_SERVER_LOCATION_FAILED, serverId));
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

  private Channel getChannel(Location loc) throws Exception {
    return channelManager.getOrCreateChannel(new Location(loc.getIp(), loc.getPort() + 1));
  }

  private GenericObjectPool<Channel> getChannelPool(Location loc) throws InterruptedException {
    return channelManager.getOrCreateChannelPool(new Location(loc.getIp(), loc.getPort() + 1), PSAgentContext
            .get()
            .getConf()
            .getInt(AngelConf.ANGEL_WORKER_TASK_NUMBER,
                AngelConf.DEFAULT_ANGEL_WORKER_TASK_NUMBER));
  }

  private void returnBuffer(Request item) {
    ByteBuf buf = item.getContext().getSerializedData();
    if(buf != null && buf.refCnt() > 0) {
      try {
        buf.release();
      } catch (Throwable x) {
        LOG.error("Release Buffer failed." , x);
      }
      item.getContext().setSerializedData(null);
    }
  }

  private void returnChannel(Request item) {
    try {
      if (item.getContext().getChannelPool() != null && item.getContext().getChannel() != null) {
        item.getContext().getChannelPool().returnObject(item.getContext().getChannel());
        item.getContext().setChannelPool(null);
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
    public final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public void getChannelSuccess() {
      try {
        lock.writeLock().lock();
        successCounter++;
      } finally {
        lock.writeLock().unlock();
      }
    }

    public void getChannelFailed() {
      try {
        lock.writeLock().lock();
        failedCounter++;
      } finally {
        lock.writeLock().unlock();
      }
    }

    public void channelNotactive() {
      try {
        lock.writeLock().lock();
        notactiveCounter++;
      } finally {
        lock.writeLock().unlock();
      }
    }

    public void reset() {
      try {
        lock.writeLock().lock();
        failedCounter = 0;
        successCounter = 0;
        notactiveCounter = 0;
        lastCheckTs = System.currentTimeMillis();
      } finally {
        lock.writeLock().unlock();
      }
    }

    public int getFailedCounter() {
      try {
        lock.readLock().lock();
        return failedCounter;
      } finally {
        lock.readLock().unlock();
      }
    }

    public int getSuccessCounter() {
      try {
        lock.readLock().lock();
        return successCounter;
      } finally {
        lock.readLock().unlock();
      }
    }

    public int getInactiveCounter() {
      try {
        lock.readLock().lock();
        return notactiveCounter;
      } finally {
        lock.readLock().unlock();
      }
    }

    public long getLastCheckTs() {
      try {
        lock.readLock().lock();
        return lastCheckTs;
      } finally {
        lock.readLock().unlock();
      }
    }
  }

  private GetChannelContext getChannelContext(PSLocation psLoc) {
    GetChannelContext context = psLocToGetChannelContextMap.get(psLoc);
    if(context == null) {
      context = psLocToGetChannelContextMap.putIfAbsent(psLoc, new GetChannelContext());
      if(context == null) {
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
     * @param seqId request id
     * @param request request context
     * @throws InterruptedException
     */
    private void sendRequest(int seqId, Request request) throws InterruptedException {
      long startTs = System.currentTimeMillis();

      // Get server id and location for this request
      PSLocation psLoc = getPSLoc(request);
      request.getContext().setActualServerId(psLoc.psId);
      request.getContext().setLocation(psLoc.loc);

      // If location is null, means that the server is not ready
      if (psLoc.loc == null) {
        LOG.error("request " + request + " server " + request.getContext().getServerId() + " location is null");
        if (psLoc.loc == null) {
          requestFailed(seqId, ResponseType.SERVER_NOT_READY, "location is null");
          return;
        }
      }

      if(LOG.isDebugEnabled() && request instanceof PartitionRequest) {
        LOG.debug("request " + request + " with seqId=" + seqId + " get location use time " + (System.currentTimeMillis() - startTs));
      }

      // Get the channel for the location
      startTs = System.currentTimeMillis();
      Channel channel;
      try {
        channel = getChannel(request, psLoc);
      } catch (Throwable e) {
        if(!stopped.get()) {
          getChannelContext(psLoc).getChannelFailed();
          LOG.error("get channel for " + psLoc.loc + " failed ", e);
          requestFailed(seqId, ResponseType.NETWORK_ERROR, StringUtils.stringifyException(e));
        }
        return;
      }

      getChannelContext(psLoc).getChannelSuccess();

      if(LOG.isDebugEnabled() && (request instanceof PartitionRequest)) {
        LOG.debug("request with seqId=" + seqId + " get channel use time " + (System.currentTimeMillis() - startTs));
      }

      // Check if need get token first
      int token = 0;
      if(request instanceof PartitionRequest) {
        if(psIdToStateMap.get(psLoc.psId) == ServerState.BUSY) {
          requestFailed(seqId, ResponseType.SERVER_IS_BUSY, "server " + psLoc.psId + " is busy");
          return;
        } else if(psIdToStateMap.get(psLoc.psId) == ServerState.GENERAL) {
          try {
            LOG.debug("PS " + psLoc + " is in GENERAL, need get token first");
            token = getToken(psLoc.loc);
            LOG.debug("token=" + token);
            if(token == 0) {
              requestFailed(seqId, ResponseType.SERVER_IS_BUSY, "PS " + psLoc.psId + " is busy");
              return;
            }
            ((PartitionRequest) request).setTokenNum(token);
          } catch (Throwable e) {
            LOG.error("get token from PS " + psLoc.loc + " failed ", e);
            requestFailed(seqId, ResponseType.NETWORK_ERROR, StringUtils.stringifyException(e));
            return;
          }
        }
      } else {
        token = 0;
      }

      // Serialize the request
      startTs = System.currentTimeMillis();
      ByteBuf buffer;
      try {
        buffer = serializeRequest(request, seqId, token);
      } catch (Throwable e) {
        if(e instanceof OutOfMemoryError) {
          rpcContext.oom();
        }

        LOG.error("serialize request " + request + " failed ", e);
        requestFailed(seqId, ResponseType.OOM, StringUtils.stringifyException(e));
        return;
      }

      if(LOG.isDebugEnabled() && (request instanceof PartitionRequest)) {
        LOG.debug("request with seqId=" + seqId + " serialize request use time " + (System.currentTimeMillis() - startTs));
      }

      // Send the request
      request.getContext().setSendStartTs(System.currentTimeMillis());
      ChannelFuture cf = channel.writeAndFlush(buffer);
      cf.addListener(new RequesterChannelFutureListener(seqId, request));

      seqIdToSendCFMap.put(seqId, new SendResultKey(
        new PSLocation(request.getContext().getActualServerId(), request.getContext().getLocation()), cf));
    }

    private int getToken(Location loc) throws IOException, ServiceException {
      return PSAgentContext.get().getPSControlClientManager().getOrCreatePSClient(loc).getToken(0);
    }

    private PSLocation getPSLoc(Request request) {
      ParameterServerId serverId = null;
      Location loc = null;
      if(request instanceof GetClocksRequest){
        serverId = ((GetClocksRequest) request).getServerId();
        loc = PSAgentContext.get().getLocationManager().getPsLocation(serverId);
        if(loc == null) {
          try {
            loc = PSAgentContext.get().getLocationManager().getPsLocation(serverId, true);
          } catch (Throwable e) {
            LOG.error("Get location from Master failed ", e);
          }
        }
      } else {
        PartitionLocation partLoc;
        try {
          partLoc = PSAgentContext.get().getMatrixMetaManager().getPartLocation(
            ((PartitionRequest) request).getPartKey(), disableRouterCache);
        } catch (Throwable e) {
          LOG.error("Get partition location from Master failed ", e);
          partLoc = PSAgentContext.get().getMatrixMetaManager().getPartLocation(
            ((PartitionRequest) request).getPartKey());
        }

        if(partLoc != null && !partLoc.psLocs.isEmpty()) {
          serverId = partLoc.psLocs.get(0).psId;
          loc = partLoc.psLocs.get(0).loc;
        }

        if(loc == null && !disableRouterCache) {
          try {
            partLoc = PSAgentContext.get().getMatrixMetaManager().getPartLocation(
              ((PartitionRequest) request).getPartKey(), true);
          } catch (Throwable e) {
            LOG.error("Get partition location from Master failed ", e);
          }

          if(partLoc != null && !partLoc.psLocs.isEmpty()) {
            serverId = partLoc.psLocs.get(0).psId;
            loc = partLoc.psLocs.get(0).loc;
          }
        }
      }

      return new PSLocation(serverId, loc);
    }

    private Channel getChannel(Request request, PSLocation psLoc) throws Exception{
      // get a channel to server from pool
      Channel channel;
      GenericObjectPool<Channel> pool;

      long startTs = System.currentTimeMillis();
      pool = getChannelPool(psLoc.loc);
      if(LOG.isDebugEnabled() && (request instanceof PartitionRequest)) {
        LOG.debug("request with seqId=" + seqId + " pool=" + pool.getNumActive());
      }

      channel = pool.borrowObject();
      if(LOG.isDebugEnabled() && (request instanceof PartitionRequest)) {
        LOG.debug("request with seqId=" + seqId + " wait for channel use time " + (
          System.currentTimeMillis() - startTs));
      }

      // if channel is not valid, it means maybe the connections to the server are closed
      if (!channel.isActive() || !channel.isOpen()) {
        getChannelContext(psLoc).channelNotactive();
        LOG.error("channel " + channel + " is not active");
        throw new IOException("channel " + channel + " is not active");
      }

      request.getContext().setChannelPool(pool);
      request.getContext().setChannel(channel);
      return channel;
    }
  }

  private ByteBuf serializeRequest(Request request, int seqId, int token) {
    // Allocate the bytebuf and serialize the request
    ByteBuf buffer = ByteBufUtils.newByteBuf(16 + request.bufferLen(), useDirectBuffer);
    buffer.writeInt(PSAgentContext.get().getPSAgentId());
    buffer.writeInt(token);
    buffer.writeInt(seqId);
    buffer.writeInt(request.getType().getMethodId());
    request.serialize(buffer);
    request.getContext().setSerializedData(buffer);
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
      if(request instanceof PartitionRequest) {
        LOG.debug("send request with seqId=" + seqId + " complete");
      }

      if (!future.isSuccess()) {
        LOG.error("send request " + seqId + " failed ", future.cause());
        requestFailed(seqId, ResponseType.NETWORK_ERROR, "send request failed " + future.cause().toString());
      } else {
        returnChannel(request);
      }
    }
  }

  public class ResponseDispatcher extends Thread {
    @Override
    public void run(){
      try {
        while (!stopped.get() && !Thread.interrupted()) {
          responseThreadPool.execute(new Responser(msgQueue.take()));
        }
      } catch (InterruptedException e) {
        if(!stopped.get()) {
          LOG.fatal("ResponseDispatcher is interrupted!! ", e);
          PSAgentContext.get().getPsAgent().error("ResponseDispatcher is interrupted!!");
        }
      }
      catch (Throwable e) {
        LOG.fatal("ResponseDispatcher is failed ", e);
        PSAgentContext.get().getPsAgent().error("ResponseDispatcher is failed. " + e.getMessage());
      }
    }
  }

  /**
   * RPC responses handler
   */
  public class Responser extends Thread {
    private ByteBuf msg;

    public Responser(ByteBuf msg) {
      this.msg = msg;
    }

    @Override public void run() {
      int seqId = 0;
      Request request = null;
      try {
        seqId = msg.readInt();

        // find the partition request context from cache
        request = seqIdToRequestMap.get(seqId);
        long startTs = System.currentTimeMillis();
        if (request == null) {
          return;
        }

        TransportMethod method = request.getType();

        if(LOG.isDebugEnabled() && (request instanceof PartitionRequest)) {
          LOG.debug("response handler, seqId=" + seqId + ", method=" + method + ", ts=" + System
            .currentTimeMillis());
        }

        switch (method) {
          case GET_ROWSPLIT: {
            handleGetRowSplitResponse(msg, seqId, (GetRowSplitRequest) request);
            break;
          }

          case GET_ROWSSPLIT: {
            handleGetRowsSplitResponse(msg, seqId, (GetRowsSplitRequest) request);
            break;
          }

          case GET_PART: {
            handleGetPartitionResponse(msg, seqId, (GetPartitionRequest) request);
            break;
          }

          case GET_CLOCKS:
            handleGetClocksResponse(msg, seqId, (GetClocksRequest) request);
            break;

          case PUT_PARTUPDATE:
            handlePutPartUpdateResponse(msg, seqId, (PutPartitionUpdateRequest) request);
            break;

          case UPDATER:
            handleUpdaterResponse(msg, seqId, (UpdaterRequest) request);
            break;

          case GET_UDF:
            handleGetUDFResponse(msg, seqId, (GetUDFRequest) request);
            break;

          default:
            break;
        }

        msg.release();

        if(LOG.isDebugEnabled() && (request instanceof PartitionRequest)) {
          LOG.debug(
            "handle response of request " + request + " use time=" + (System.currentTimeMillis()
              - startTs));
        }
      } catch (InterruptedException ie) {
        LOG.warn(Thread.currentThread().getName() + " is interruptted");
      } catch (Throwable x) {
        LOG.error("hanlder rpc response failed ", x);
        if(x instanceof OutOfMemoryError) {
          rpcContext.oom();
          requestFailed(seqId, ResponseType.OOM, StringUtils.stringifyException(x));
        } else {
          requestFailed(seqId, ResponseType.UNKNOWN_ERROR, StringUtils.stringifyException(x));
        }
      }
    }
    
    @SuppressWarnings("unchecked")
    private void handleGetUDFResponse(ByteBuf buf, int seqId, GetUDFRequest request) {
      GetUDFResponse response = new GetUDFResponse();
      response.deserialize(buf);

      handleServerState(request, response.getState());

      if (response.getResponseType() == ResponseType.SUCCESS) {
        FutureResult<PartitionGetResult> future = requestToResultMap.remove(request);
        if (future != null) {
          future.set(response.getPartResult());
        }
        requestSuccess(seqId);
      } else {
        handleExceptionResponse(seqId, request, response);
      }
    }

    @SuppressWarnings("unchecked")
    private void handleUpdaterResponse(ByteBuf buf, int seqId, UpdaterRequest request) {
      UpdaterResponse response = new UpdaterResponse();
      response.deserialize(buf);

      handleServerState(request, response.getState());

      if (response.getResponseType() == ResponseType.SUCCESS) {
        FutureResult<VoidResult> future = requestToResultMap.remove(request);
        if (future != null) {
          future.set(new VoidResult(com.tencent.angel.psagent.matrix.ResponseType.SUCCESS));
        }
        requestSuccess(seqId);
      } else {
        handleExceptionResponse(seqId, request, response);
      }
    }

    @SuppressWarnings("unchecked")
    private void handlePutPartUpdateResponse(ByteBuf buf, int seqId,
        PutPartitionUpdateRequest request) {
      PutPartitionUpdateResponse response = new PutPartitionUpdateResponse();
      response.deserialize(buf);

      handleServerState(request, response.getState());

      if (response.getResponseType() == ResponseType.SUCCESS) {
        FutureResult<VoidResult> future = requestToResultMap.remove(request);
        if (future != null) {
          future.set(new VoidResult(com.tencent.angel.psagent.matrix.ResponseType.SUCCESS));
        }
        requestSuccess(seqId);
      }else {
        handleExceptionResponse(seqId, request, response);
      }
    }

    @SuppressWarnings("unchecked")
    private void handleGetClocksResponse(ByteBuf buf, int seqId, GetClocksRequest request) {
      GetClocksResponse response = new GetClocksResponse();
      response.deserialize(buf);

      handleServerState(request, response.getState());

      FutureResult<GetClocksResponse> future = requestToResultMap.remove(request);
      if (future != null) {
        future.set(response);
      }

      if (response.getResponseType() == ResponseType.SUCCESS) {
        requestSuccess(seqId);
      } else {
        handleExceptionResponse(seqId, request, response);
      }
    }

    @SuppressWarnings("unchecked")
    private void handleGetPartitionResponse(ByteBuf buf, int seqId, GetPartitionRequest request) {
      GetPartitionResponse response = new GetPartitionResponse();
      response.deserialize(buf);

      handleServerState(request, response.getState());

      if (response.getResponseType() == ResponseType.SUCCESS) {
        updateMatrixCache(request.getPartKey(), response.getPartition());
        FutureResult<ServerPartition> future = requestToResultMap.remove(request);
        if (future != null) {
          future.set(response.getPartition());
        }
        requestSuccess(seqId);
      } else {
        handleExceptionResponse(seqId, request, response);
      }
    }

    @SuppressWarnings("unchecked")
    private void handleGetRowsSplitResponse(ByteBuf buf, int seqId, GetRowsSplitRequest request) {
      GetRowsSplitResponse response = new GetRowsSplitResponse();
      response.deserialize(buf);

      handleServerState(request, response.getState());

      if (response.getResponseType() == ResponseType.SUCCESS) {
        updateMatrixCache(request.getPartKey(), response.getRowsSplit());
        FutureResult<List<ServerRow>> future = requestToResultMap.remove(request);
        if (future != null) {
          future.set(response.getRowsSplit());
        }
        requestSuccess(seqId);
      } else {
        handleExceptionResponse(seqId, request, response);
      }
    }

    @SuppressWarnings("unchecked")
    private void handleGetRowSplitResponse(ByteBuf buf, int seqId, GetRowSplitRequest request)
        throws InterruptedException {
      GetRowSplitResponse response = new GetRowSplitResponse();
      ServerRow rowSplit =
          PSAgentContext
              .get()
              .getMatricesCache()
              .getRowSplit(request.getPartKey().getMatrixId(), request.getPartKey(),
                  request.getRowIndex());
      response.setRowSplit(rowSplit);
      response.deserialize(buf);

      handleServerState(request, response.getState());

      if (response.getResponseType() == ResponseType.SUCCESS) {
        updateMatrixCache(request.getPartKey(), response.getRowSplit());
        FutureResult<ServerRow> future = requestToResultMap.remove(request);
        if (future != null) {
          future.set(response.getRowSplit());
        }
        requestSuccess(seqId);
      } else {
        handleExceptionResponse(seqId, request, response);
      }
    }

    private void handleExceptionResponse(int seqId, Request request, Response response) {
      LOG.error("request " + request + " has a exception response " + response);
      switch (response.getResponseType()) {
        case CLOCK_NOTREADY:
        case SERVER_IS_BUSY:
        case SERVER_HANDLE_FAILED:
          requestFailed(seqId, response.getResponseType(), response.getDetail());
          break;

        case SERVER_HANDLE_FATAL:
          handleFatalError(seqId, request, response);
          break;

        default:
          break;
      }
    }

    private void handleFatalError(int seqId, Request request, Response response) {
      String errorMsg = "get row split fatal error happened " + response.getDetail();
      LOG.fatal(errorMsg);
      PSAgentContext.get().getPsAgent().error(errorMsg);
    }

    private void handleServerState(Request request, ServerState state) {
      if(LOG.isDebugEnabled() && (request instanceof PartitionRequest)) {
        LOG.debug("request " + request + " response state = " + state);
      }
      if(state != null) {
        psIdToStateMap.put(request.getContext().getActualServerId(), state);
      }
    }

    private void updateMatrixCache(PartitionKey partKey, ServerPartition partition) {
      PSAgentContext.get().getMatricesCache().update(partKey.getMatrixId(), partKey, partition);
    }

    private void updateMatrixCache(PartitionKey partKey, ServerRow rowSplit) {
      PSAgentContext.get().getMatricesCache().update(partKey.getMatrixId(), partKey, rowSplit);
    }

    private void updateMatrixCache(PartitionKey partKey, List<ServerRow> rowsSplit) {
      PSAgentContext.get().getMatricesCache().update(partKey.getMatrixId(), partKey, rowsSplit);
    }
  }

  @Override
  public Future<VoidResult> update(UpdateFunc updateFunc,
      PartitionUpdateParam partitionUpdaterParam) {
    ParameterServerId serverId =
        PSAgentContext.get().getMatrixMetaManager().getMasterPS((partitionUpdaterParam.getPartKey()));

    UpdaterRequest request =
        new UpdaterRequest(partitionUpdaterParam.getPartKey(), updateFunc.getClass()
            .getName(), partitionUpdaterParam);

    LOG.debug("update request=" + request);

    FutureResult<VoidResult> future = new FutureResult<VoidResult>();
    requestToResultMap.put(request, future);

    addToPutQueueForServer(serverId, request);
    startPut();
    return future;
  }
      
  @SuppressWarnings("unchecked")
  @Override
  public Future<PartitionGetResult> get(GetFunc func, PartitionGetParam partitionGetParam) {
    ParameterServerId serverId =
        PSAgentContext.get().getMatrixMetaManager().getMasterPS((partitionGetParam.getPartKey()));

    GetUDFRequest request =
        new GetUDFRequest(partitionGetParam.getPartKey(),
            func.getClass().getName(), partitionGetParam);

    LOG.debug("get request=" + request);

    FutureResult<PartitionGetResult> future = new FutureResult<PartitionGetResult>();
    FutureResult<PartitionGetResult> oldFuture = requestToResultMap.putIfAbsent(request, future);
    if (oldFuture != null) {
      LOG.debug("same request exist, just return old future");
      return oldFuture;
    } else {
      addToGetQueueForServer(serverId, request);
      startGet();
      return future;
    }
  }
}
