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
import com.tencent.angel.common.Location;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.ml.matrix.psf.update.enhance.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.transport.*;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.enhance.VoidResult;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;
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

import java.nio.ByteOrder;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
  private final ExecutorService clientThreadPool;

  /** response message handler */
  private Responser responser;

  private final ArrayList<Long> getUseTimes;

  /** channel pool manager:it maintain a channel pool for every server */
  private ChannelManager channelManager;

  /** use direct netty buffer or not */
  private final boolean useDirectBuffer;

  /**
   * Create a new MatrixTransportClient.
   */
  @SuppressWarnings("rawtypes")
  public MatrixTransportClient() {
    seqIdToRequestMap = new ConcurrentHashMap<Integer, Request>();
    requestToResultMap = new ConcurrentHashMap<Request, FutureResult>();

    dispatchMessageQueue = new LinkedBlockingQueue<DispatcherEvent>();
    getItemQueues = new ConcurrentHashMap<ParameterServerId, LinkedBlockingQueue<Request>>();
    putItemQueues = new ConcurrentHashMap<ParameterServerId, LinkedBlockingQueue<Request>>();

    getUseTimes = new ArrayList<Long>();

    msgQueue = new LinkedBlockingQueue<ByteBuf>();
    stopped = new AtomicBoolean(false);
    clientThreadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() / 2);

    Configuration conf = PSAgentContext.get().getConf();
    timer = new Timer();
    checkPeriodMS =
        conf.getInt(AngelConfiguration.ANGEL_MATRIXTRANSFER_CHECK_INTERVAL_MS,
            AngelConfiguration.DEFAULT_ANGEL_MATRIXTRANSFER_CHECK_INTERVAL_MS);

    retryIntervalMs =
        conf.getInt(AngelConfiguration.ANGEL_MATRIXTRANSFER_RETRY_INTERVAL_MS,
            AngelConfiguration.DEFAULT_ANGEL_MATRIXTRANSFER_RETRY_INTERVAL_MS);

    useDirectBuffer =
        conf.getBoolean(AngelConfiguration.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_USEDIRECTBUFFER,
            AngelConfiguration.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_USEDIRECTBUFFER);

    channelManager = null;
  }

  private void init() {
    Configuration conf = PSAgentContext.get().getConf();
    int workerNum =
        conf.getInt(AngelConfiguration.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_EVENTGROUP_THREADNUM,
            AngelConfiguration.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_EVENTGROUP_THREADNUM);

    int sendBuffSize =
        conf.getInt(AngelConfiguration.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_SNDBUF,
            AngelConfiguration.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_SNDBUF);

    int recvBuffSize =
        conf.getInt(AngelConfiguration.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_RCVBUF,
            AngelConfiguration.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_RCVBUF);
    
    final int maxMessageSize = 
        conf.getInt(AngelConfiguration.ANGEL_NETTY_MATRIXTRANSFER_MAX_MESSAGE_SIZE,
            AngelConfiguration.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_MAX_MESSAGE_SIZE);

    bootstrap = new Bootstrap();
    channelManager = new ChannelManager(bootstrap);
    eventGroup = new NioEventLoopGroup(workerNum);
    bootstrap.group(eventGroup).channel(NioSocketChannel.class)
        .option(ChannelOption.SO_SNDBUF, sendBuffSize)
        .option(ChannelOption.SO_RCVBUF, recvBuffSize)
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeLine = ch.pipeline();
            pipeLine.addLast(new LengthFieldBasedFrameDecoder(maxMessageSize, 0, 4, 0, 4));
            pipeLine.addLast(new LengthFieldPrepender(4));
            pipeLine.addLast(new MatrixTransportClientHandler(msgQueue, dispatchMessageQueue));
          }
        });
  }

  /**
   * Start the task dispatcher, rpc responses handler and the time clock generator.
   */
  public void start() {
    init();

    requestDispacher = new RequestDispatcher();
    requestDispacher.start();

    responser = new Responser();
    responser.start();

    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          periodCheck();
        } catch (InterruptedException e) {

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

      if (responser != null) {
        responser.interrupt();
        responser = null;
      }

      if (channelManager != null) {
        channelManager.clear();
      }
      eventGroup.shutdownGracefully();
      clientThreadPool.shutdownNow();
    }
  }

  private void periodCheck() throws InterruptedException {
    dispatchMessageQueue.put(new DispatcherEvent(EventType.PERIOD_CHECK));
  }

  @Override
  public Future<ServerPartition> getPart(PartitionKey partKey, int clock) {
    ParameterServerId serverId = PSAgentContext.get().getMatrixPartitionRouter().getPSId(partKey);
    GetPartitionRequest request = new GetPartitionRequest(serverId, partKey, clock);

    FutureResult<ServerPartition> future = new FutureResult<ServerPartition>();
    requestToResultMap.put(request, future);
    addToGetQueueForServer(serverId, request);
    startGet();
    return future;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Future<ServerRow> getRowSplit(PartitionKey partKey, int rowIndex, int clock) {

    ParameterServerId serverId = PSAgentContext.get().getMatrixPartitionRouter().getPSId(partKey);
    GetRowSplitRequest request = new GetRowSplitRequest(serverId, clock, partKey, rowIndex);
    LOG.debug("getRowSplit request=" + request);

    FutureResult<ServerRow> future = new FutureResult<ServerRow>();
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
    ParameterServerId serverId = PSAgentContext.get().getMatrixPartitionRouter().getPSId(partKey);
    GetRowsSplitRequest request = new GetRowsSplitRequest(serverId, clock, partKey, rowIndexes);
    FutureResult<List<ServerRow>> future = new FutureResult<List<ServerRow>>();
    requestToResultMap.put(request, future);
    addToGetQueueForServer(serverId, request);
    startGet();
    return future;
  }

  @Override
  public Future<VoidResult> putPart(PartitionKey partKey, List<RowUpdateSplit> rowsSplit,
      int taskIndex, int clock, boolean updateClock) {
    ParameterServerId serverId = PSAgentContext.get().getMatrixPartitionRouter().getPSId(partKey);
    PutPartitionUpdateRequest request =
        new PutPartitionUpdateRequest(serverId, taskIndex, clock, partKey, rowsSplit, updateClock);

    FutureResult<VoidResult> future = new FutureResult<VoidResult>();
    requestToResultMap.put(request, future);
    addToPutQueueForServer(serverId, request);
    startPut();
    return future;
  }

  @Override
  public Future<Map<PartitionKey, Integer>> getClocks(ParameterServerId serverId) {
    GetClocksRequest request = new GetClocksRequest(serverId);
    FutureResult<Map<PartitionKey, Integer>> future =
        new FutureResult<Map<PartitionKey, Integer>>();
    requestToResultMap.put(request, future);
    addToGetQueueForServer(serverId, request);
    startGet();
    return future;
  }

  private void addToGetQueueForServer(ParameterServerId serverId, Request request) {
    LinkedBlockingQueue<Request> queue = getItemQueues.get(serverId);
    if (queue == null) {
      queue = new LinkedBlockingQueue<Request>();
      getItemQueues.putIfAbsent(serverId, queue);
      queue = getItemQueues.get(serverId);
    }
    queue.add(request);
  }

  private void addToPutQueueForServer(ParameterServerId serverId, Request request) {
    LinkedBlockingQueue<Request> queue = putItemQueues.get(serverId);
    if (queue == null) {
      queue = new LinkedBlockingQueue<Request>();
      putItemQueues.putIfAbsent(serverId, queue);
      queue = putItemQueues.get(serverId);
    }
    queue.add(request);
  }

  /**
   * RPC request dispatcher.
   */
  class RequestDispatcher extends Thread {
    /** max bytes of the flight get/put request */
    private final long maxBytesInFlight;

    /** total data bytes of the flight put requests */
    private final AtomicInteger totalBytesInFlightPut;

    /** total data bytes of the flight get requests */
    private final AtomicInteger totalBytesInFlightGet;

    /** max number of the flight requests */
    private final int maxReqNumInFlight;

    /** total flight request counter */
    private final AtomicInteger totalRequestNumInFlightAtomic;

    /** max number of the flight requests to each ps */
    private final int maxReqNumInFlightPerServer;

    /** ps id to flight request counter map */
    private final Map<ParameterServerId, Integer> reqNumInFlightCounters;

    private int totalRequestNumInFlight;

    /**
     * if partition request to a parameterserver continuous failure, we should refresh the location
     * for it
     */
    private final int refreshThreshold;

    /** server id to the number of falied request counter map */
    private final Map<ParameterServerId, Integer> serverFailedStatics;

    /**
     * schedulable failed put request queue: the requests in the will be scheduled first
     */
    private final LinkedBlockingQueue<Request> schedulableFailedPutCache;

    /**
     * schedulable failed get request queue: the requests in the will be scheduled first
     */
    private final LinkedBlockingQueue<Request> schedulableFailedGetCache;

    private final LinkedList<Request> waitGetList;

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
    private final HashSet<ParameterServerId> refreshingServerSet;

    private final int requestTimeOut;

    private int tickClock;
    private int currentSeqId;

    public RequestDispatcher() {
      Configuration conf = PSAgentContext.get().getConf();
      psIds = PSAgentContext.get().getLocationCache().getPSIds();
      lastChosenPutServerIndex = new Random().nextInt(psIds.length);
      lastChosenGetServerIndex = new Random().nextInt(psIds.length);

      totalBytesInFlightPut = new AtomicInteger(0);
      totalBytesInFlightGet = new AtomicInteger(0);
      totalRequestNumInFlightAtomic = new AtomicInteger(0);
      maxBytesInFlight = PSAgentContext.get().getMaxBytesInFlight();
      int serverNum =
          conf.getInt(AngelConfiguration.ANGEL_PS_NUMBER,
              AngelConfiguration.DEFAULT_ANGEL_PS_NUMBER);

      maxReqNumInFlightPerServer =
          conf.getInt(AngelConfiguration.ANGEL_MATRIXTRANSFER_MAX_REQUESTNUM_PERSERVER,
              AngelConfiguration.DEFAULT_ANGEL_MATRIXTRANSFER_MAX_REQUESTNUM_PERSERVER);

      reqNumInFlightCounters = new HashMap<ParameterServerId, Integer>();
      for (int i = 0; i < psIds.length; i++) {
        reqNumInFlightCounters.put(psIds[i], 0);
      }

      int maxReqNumInFlight =
          conf.getInt(AngelConfiguration.ANGEL_MATRIXTRANSFER_MAX_REQUESTNUM,
              AngelConfiguration.DEFAULT_ANGEL_MATRIXTRANSFER_MAX);

      if (maxReqNumInFlight > serverNum * maxReqNumInFlightPerServer) {
        maxReqNumInFlight = serverNum * maxReqNumInFlightPerServer;
      }
      this.maxReqNumInFlight = maxReqNumInFlight;

      totalRequestNumInFlight = 0;

      failedPutCache = new LinkedList<Request>();
      failedGetCache = new LinkedList<Request>();
      schedulableFailedPutCache = new LinkedBlockingQueue<Request>();
      schedulableFailedGetCache = new LinkedBlockingQueue<Request>();
      waitGetList = new LinkedList<Request>();
      serverFailedStatics = new HashMap<ParameterServerId, Integer>();

      refreshThreshold =
          conf.getInt(AngelConfiguration.ANGEL_REFRESH_SERVERLOCATION_THRESHOLD,
              AngelConfiguration.DEFAULT_ANGEL_REFRESH_SERVERLOCATION_THRESHOLD);

      refreshingServerSet = new HashSet<ParameterServerId>();

      requestTimeOut =
          conf.getInt(AngelConfiguration.ANGEL_MATRIXTRANSFER_REQUEST_TIMEOUT_MS,
              AngelConfiguration.DEFAULT_ANGEL_MATRIXTRANSFER_REQUEST_TIMEOUT_MS);

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
              updateGetFlowControl(request, -1);
              getDataSplit();
              break;
            }

            case GET_NOTREADY: {
              Request request = ((RequestDispatchEvent) event).getRequest();
              updateGetFlowControl(request, -1);
              waitGetList.add(request);
              getDataSplit();
              break;
            }

            case GET_FAILED: {
              Request request = ((RequestDispatchEvent) event).getRequest();
              request.getContext().setFailedTs(System.currentTimeMillis());
              updateGetFlowControl(request, -1);

              failedGetCache.add(request);
              Integer counter = serverFailedStatics.get(request.getServerId());
              if (counter == null) {
                serverFailedStatics.put(request.getServerId(), 1);
              } else {
                serverFailedStatics.put(request.getServerId(), counter + 1);
              }

              if (serverFailedStatics.get(request.getServerId()) >= refreshThreshold) {
                addRefreshingServer(request.getServerId());
                closeChannelForServer(request.getServerId());
                refreshServerLocation(request.getServerId());
                serverFailedStatics.put(request.getServerId(), 0);
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
              updatePutFlowControl(request, -1);
              putDataSplit();
              break;
            }

            case PUT_FAILED: {
              Request request = ((RequestDispatchEvent) event).getRequest();
              request.getContext().setFailedTs(System.currentTimeMillis());
              updatePutFlowControl(request, -1);

              failedPutCache.add(request);
              Integer counter = serverFailedStatics.get(request.getServerId());
              if (counter == null) {
                serverFailedStatics.put(request.getServerId(), 1);
              } else {
                serverFailedStatics.put(request.getServerId(), counter + 1);
              }

              if (serverFailedStatics.get(request.getServerId()) >= refreshThreshold) {
                addRefreshingServer(request.getServerId());
                closeChannelForServer(request.getServerId());
                refreshServerLocation(request.getServerId());
                serverFailedStatics.put(request.getServerId(), 0);
              }

              putDataSplit();
              break;
            }

            case REFRESH_SERVER_LOCATION_SUCCESS: {
              ParameterServerId serverId = ((RefreshServerLocationEvent) event).getServerId();
              removeRefreshingServer(serverId);
              dispatchTransportEvent(serverId);
              break;
            }

            case REFRESH_SERVER_LOCATION_FAILED: {
              ParameterServerId serverId = ((RefreshServerLocationEvent) event).getServerId();
              removeRefreshingServer(serverId);
              LOG.fatal("get server location from master failed");
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
          LOG.warn("RequestDispacher exit as interrupt");
        }
      }
    }

    private void updateGetFlowControl(Request request, int factor) {
      updateGetBytesInFlight(request, factor);
      updateReqNumInFlight(request.getServerId(), factor);
    }

    private void updatePutFlowControl(Request request, int factor) {
      updatePutBytesInFlight(request, factor);
      updateReqNumInFlight(request.getServerId(), factor);
    }


    private void addRefreshingServer(ParameterServerId serverId) {
      refreshingServerSet.add(serverId);
    }

    private void removeRefreshingServer(ParameterServerId serverId) {
      refreshingServerSet.remove(serverId);
    }

    private boolean isRefreshing(ParameterServerId serverId) {
      return refreshingServerSet.contains(serverId);
    }

    /**
     * choose get partition requests and send it to server first schedule schedulableFailedGetCache
     */
    private void getDataSplit() {
      if (checkIsOverReqNumLimit()) {
        return;
      }

      if (submitGetTask(schedulableFailedGetCache) == 0) {
        return;
      }

      LinkedBlockingQueue<Request> getQueue = null;
      while (true) {
        getQueue = chooseGetQueue();
        if (getQueue == null) {
          return;
        }

        // if submit task in getQueue failed, we should make up the last chosen get queue index
        if (submitGetTask(getQueue) == 0) {
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
        if (!isRefreshing(psIds[index]) && !checkIsOverReqNumLimit(psIds[index])) {
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
     * @param getQueue
     * @return
     */
    private int submitGetTask(LinkedBlockingQueue<Request> getQueue) {
      Request item;
      if ((item = getQueue.peek()) != null) {
        if (isRefreshing(item.getServerId())) {
          return 1;
        }

        if (!checkIsOverGetFlowControlLimit(item) && !checkIsOverReqNumLimit(item.getServerId())) {
          item = getQueue.poll();
          updateGetFlowControl(item, 1);
          submit(item);
          return 1;
        } else {
          return 0;
        }
      } else {
        return 1;
      }
    }

    private boolean checkIsOverReqNumLimit() {
      return ((totalRequestNumInFlight + 1) > maxReqNumInFlight);
    }

    private boolean checkIsOverReqNumLimit(ParameterServerId serverId) {
      return (reqNumInFlightCounters.get(serverId) + 1) > maxReqNumInFlightPerServer;
    }

    private void updateReqNumInFlight(ParameterServerId serverId, int factor) {
      totalRequestNumInFlight += factor;
      totalRequestNumInFlightAtomic.addAndGet(factor);
      reqNumInFlightCounters.put(serverId, reqNumInFlightCounters.get(serverId) + factor);
    }

    private boolean checkIsOverGetFlowControlLimit(Request request) {
      return request.getEstimizeDataSize() + totalBytesInFlightGet.get() > maxBytesInFlight;
    }

    private void updateGetBytesInFlight(Request request, int factor) {
      totalBytesInFlightGet.addAndGet(request.getEstimizeDataSize() * factor);
    }

    /**
     * choose put partition requests and send it to server first schedule schedulableFailedGetCache
     */
    private void putDataSplit() {
      if (checkIsOverReqNumLimit()) {
        return;
      }

      // Then submit normal task until reach upper limit of flow control or all tasks are submit
      if (submitPutTask(schedulableFailedPutCache) == 0) {
        return;
      }

      LinkedBlockingQueue<Request> putQueue = null;
      while (true) {
        putQueue = choosePutQueue();
        if (putQueue == null) {
          return;
        }

        // if submit task in getQueue failed, we should make up the last chosen get queue index
        if (submitPutTask(putQueue) == 0) {
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

    /**
     * submit put partition requests in a queue, if a request in this queue satisfy flow-control
     * condition and the server location for this request is not refreshing, it can be submit
     * 
     * @return
     */
    private int submitPutTask(LinkedBlockingQueue<Request> putQueue) {
      Request item;
      if ((item = putQueue.peek()) != null) {
        if (isRefreshing(item.getServerId())) {
          return 1;
        }

        if (!checkIsOverPutFlowControlLimit(item) && !checkIsOverReqNumLimit(item.getServerId())) {
          item = putQueue.poll();
          updatePutFlowControl(item, 1);
          submit(item);
          return 1;
        } else {
          return 0;
        }
      } else {
        return 1;
      }
    }

    private void submit(Request item) {
      int seqId = currentSeqId++;
      item.getContext().setWaitTimeTicks(0);
      seqIdToRequestMap.put(seqId, item);
      clientThreadPool.execute(new Requester(item, seqId));
    }

    private void updatePutBytesInFlight(Request request, int factor) {
      totalBytesInFlightPut.addAndGet(request.getEstimizeDataSize() * factor);
    }

    private boolean checkIsOverPutFlowControlLimit(Request request) {
      return request.getEstimizeDataSize() + totalBytesInFlightPut.get() > maxBytesInFlight;
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
      LOG.debug("dispatcheableFailedPutCache size = " + schedulableFailedPutCache.size());
      LOG.debug("dispatcheableFailedGetCache size = " + schedulableFailedGetCache.size());
      for (Entry<ParameterServerId, LinkedBlockingQueue<Request>> entry : getItemQueues.entrySet()) {
        LOG.debug("period check, for server " + entry.getKey() + ", there is "
            + entry.getValue().size() + " get items need dispatch ");
      }

      for (Entry<ParameterServerId, LinkedBlockingQueue<Request>> entry : putItemQueues.entrySet()) {
        LOG.debug("period check, for server " + entry.getKey() + ", there is "
            + entry.getValue().size() + " put items need dispatch ");
      }

      LOG.debug("dispatcheableFailedGetCache size = " + schedulableFailedGetCache.size());
      LOG.debug("totalRequestNumInFlight=" + totalRequestNumInFlight);
      LOG.debug("totalRequestNumInFlightAtomic=" + totalRequestNumInFlightAtomic.get());
      LOG.debug("maxReqNumInFlight=" + maxReqNumInFlight);
      LOG.debug("totalBytesInFlightGet.get()=" + totalBytesInFlightGet.get());
      LOG.debug("totalBytesInFlightPut.get()=" + totalBytesInFlightPut.get());
      LOG.debug("maxBytesInFlight=" + maxBytesInFlight);

      for (Entry<ParameterServerId, Integer> entry : reqNumInFlightCounters.entrySet()) {
        LOG.debug("for server " + entry.getKey() + " isRefreshing=" + isRefreshing(entry.getKey())
            + ", reqNumInFlightCounters=" + reqNumInFlightCounters.get(entry.getKey())
            + ", maxReqNumInFlightPerServer=" + maxReqNumInFlightPerServer);

      }

      for (Entry<Integer, Request> entry : seqIdToRequestMap.entrySet()) {
        LOG.debug("infight request id=" + entry.getKey() + ", request context=" + entry.getValue()
            + ", request channel=" + entry.getValue().getContext().getChannel());
      }
    }

    private void dispatchTransportEvent(ParameterServerId serverId) {
      retryGet();
      if (tickClock % 10 != 0) {
        return;
      }

      removeTimeOutRequestItem();

      long ts = System.currentTimeMillis();
      Iterator<Request> iter = failedPutCache.iterator();
      while (iter.hasNext()) {
        Request item = iter.next();
        if ((serverId == null || item.getServerId() == serverId)
            && !isRefreshing(item.getServerId())
            && (ts - item.getContext().getFailedTs() >= retryIntervalMs)) {
          schedulableFailedPutCache.add(item);
          iter.remove();
        }
      }
      putDataSplit();

      iter = failedGetCache.iterator();
      while (iter.hasNext()) {
        Request item = iter.next();
        if ((serverId == null || item.getServerId() == serverId)
            && !isRefreshing(item.getServerId())
            && (ts - item.getContext().getFailedTs() >= retryIntervalMs)) {
          schedulableFailedGetCache.add(item);
          iter.remove();
        }
      }

      getDataSplit();

      if (LOG.isDebugEnabled()) {
        printDispatchInfo();
      }
    }

    private void retryGet() {
      Iterator<Request> iter = waitGetList.iterator();
      while (iter.hasNext()) {
        Request item = iter.next();
        try {
          getItemQueues.get(item.getServerId()).add(item);
          iter.remove();
        } catch (Exception x) {
          LOG.warn("add " + item + " to getItemQueues falied, ", x);
        }
      }
    }

    private void removeTimeOutRequestItem() {
      int removeNum = 0;
      try {
        for (Entry<Integer, Request> entry : seqIdToRequestMap.entrySet()) {
          Request item = entry.getValue();
          item.getContext().addWaitTimeTicks(checkPeriodMS * 10);
          LOG.debug("request " + entry.getKey() + " wait time="
              + item.getContext().getWaitTimeTicks());
          if (item.getContext().getWaitTimeTicks() > requestTimeOut) {
            item = seqIdToRequestMap.get(entry.getKey());
            if (item != null) {
              LOG.info("remove timeout request " + item);
              removeNum++;
              requestFailed(entry.getKey(), item);
            }
          }
        }

        LOG.debug("remove timeout request, removeNum=" + removeNum);
      } catch (Exception x) {
        LOG.fatal("remove request failed ", x);
      }
    }

    /**
     * if a channel is closed, all request in this channel should be remove and reschedule
     * 
     * @param channel closed channel
     */
    private void removeRequestForChannel(Channel channel) {
      LOG.debug("remove channel " + channel);
      int removeNum = 0;
      try {
        for (Entry<Integer, Request> entry : seqIdToRequestMap.entrySet()) {
          Channel ch = entry.getValue().getContext().getChannel();
          if (ch != null && ch.equals(channel)) {
            removeNum++;
            Request item = seqIdToRequestMap.get(entry.getKey());
            if (item != null) {
              requestFailed(entry.getKey(), item);
            }
          }
        }

        LOG.debug("remove channel " + channel + ", removeNum=" + removeNum);
      } catch (Exception x) {
        LOG.fatal("remove request failed ", x);
      }
    }
  }

  private void requestNotReady(int seqId, Request request) {
    seqIdToRequestMap.remove(seqId);
    returnChannel(request);
    switch (request.getType()) {
      case GET_PART:
      case GET_ROWSPLIT:
      case GET_ROWSSPLIT:
        getRequestNotReady(request);
        break;

      default:
        LOG.error("unvalid response for request " + request + " with seqId " + seqId);
        break;
    }
  }

  private void requestSuccess(int seqId, Request request) {
    seqIdToRequestMap.remove(seqId);
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

  private void requestFailed(Integer seqId, Request request) {
    seqIdToRequestMap.remove(seqId);
    returnChannel(request);
    switch (request.getType()) {
      case GET_PART:
      case GET_ROWSPLIT:
      case GET_ROWSSPLIT:
      case GET_CLOCKS:
      case GET_UDF:
        getRequestFailed(request);
        break;

      case PUT_PART:
      case PUT_PARTUPDATE:
      case UPDATER:
        putRequestFailed(request);
        break;

      default:
        LOG.error("unvalid request " + request);
        break;
    }
  }

  private void putRequestSuccess(Request request) {
    try {
      dispatchMessageQueue.add(new RequestDispatchEvent(EventType.PUT_SUCCESS, request));
    } catch (Exception e) {
      LOG.error("add PUT_SUCCESS event for request " + request + " failed, ", e);
    }
  }

  private void getRequestSuccess(Request request) {
    try {
      dispatchMessageQueue.add(new RequestDispatchEvent(EventType.GET_SUCCESS, request));
    } catch (Exception e) {
      LOG.error("add GET_SUCCESS event for request " + request + " failed, ", e);
    }
  }

  private void getRequestNotReady(Request request) {
    try {
      dispatchMessageQueue.add(new RequestDispatchEvent(EventType.GET_NOTREADY, request));
    } catch (Exception e) {
      LOG.error("add GET_NOTREADY event for request " + request + " failed, ", e);
    }
  }

  private void getRequestFailed(Request request) {
    try {
      dispatchMessageQueue.add(new RequestDispatchEvent(EventType.GET_FAILED, request));
    } catch (Exception e) {
      LOG.error("add GET_FAILED event for request " + request + " failed, ", e);
    }
  }

  private void putRequestFailed(Request request) {
    try {
      dispatchMessageQueue.add(new RequestDispatchEvent(EventType.PUT_FAILED, request));
    } catch (Exception e) {
      LOG.error("add PUT_FAILED event for request " + request + " failed, ", e);
    }
  }

  /**
   * refresh the server location use async mode
   * 
   * @param serverId server id
   */
  private void refreshServerLocation(final ParameterServerId serverId) {
    Thread psLocRefresher = new Thread() {
      @Override
      public void run() {
        Location location = null;
        try {
          while (location == null) {
            Thread.sleep(PSAgentContext.get().getRequestSleepTimeMS());
            location = PSAgentContext.get().getMasterClient().getPSLocation(serverId);
            if(location != null) {
              PSAgentContext.get().getLocationCache().setPSLocation(serverId, location);
            }           
          }
          refreshServerLocationSuccess(serverId);
        } catch (InterruptedException | ServiceException x) {
          refreshServerLocationFailed(serverId);
        }
      }
    };

    psLocRefresher.setName("ps-location-getter");
    psLocRefresher.start();
  }

  private void refreshServerLocationSuccess(ParameterServerId serverId) {
    dispatchMessageQueue.add(new RefreshServerLocationEvent(
        EventType.REFRESH_SERVER_LOCATION_SUCCESS, serverId));
  }

  private void refreshServerLocationFailed(ParameterServerId serverId) {
    dispatchMessageQueue.add(new RefreshServerLocationEvent(
        EventType.REFRESH_SERVER_LOCATION_FAILED, serverId));
  }

  private void startGet() {
    dispatchMessageQueue.add(new DispatcherEvent(EventType.START_GET));
  }

  private void startPut() {
    dispatchMessageQueue.add(new DispatcherEvent(EventType.START_PUT));
  }

  private void closeChannelForServer(ParameterServerId serverId) {
    Location loc = PSAgentContext.get().getLocationCache().getPSLocation(serverId);
    if (loc == null) {
      return;
    }

    channelManager.removeChannelPool(loc);
  }

  private GenericObjectPool<Channel> getChannelPool(Location loc) throws InterruptedException {
    return channelManager.getChannelPool(loc);
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
      PSAgentContext.get().getLocationCache().setPSLocation(serverId, location);
    }
    return location;
  }

  public ArrayList<Long> getGetUseTimes() {
    return getUseTimes;
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

      // allocate the bytebuf
      ByteBuf buffer = ByteBufUtils.newByteBuf(request.bufferLen(), useDirectBuffer);
      buffer.writeInt(seqId);
      buffer.writeInt(request.getType().getMethodId());
      request.serialize(buffer);

      // check the location of server is ready, if not, we should wait
      Location loc = PSAgentContext.get().getLocationCache().getPSLocation(request.getServerId());
      if (loc == null) {
        LOG.debug("server " + request.getServerId() + " location is null, get from master now");
        loc = getPSLocFromMaster(request.getServerId());
        if (loc == null) {
          requestFailed(seqId, request);
          return;
        }
      }

      // get a channel to server from pool
      Channel channel = null;
      GenericObjectPool<Channel> pool = null;

      try {
        pool = getChannelPool(loc);
        channel = pool.borrowObject();

        // if channel is not valid, it means maybe the connections to the server are closed
        if (!channel.isActive() || !channel.isOpen()) {
          LOG.error("channel " + channel + " is not active");
          // channelManager.removeChannelPool(loc);
          requestFailed(seqId, request);
          return;
        }
      } catch (Exception x) {
        if(!stopped.get()) {
          LOG.error("get channel failed ", x);
          requestFailed(seqId, request);
        }
        return;
      }

      request.getContext().setChannelPool(pool);
      request.getContext().setChannel(channel);

      ChannelFuture cf = channel.writeAndFlush(buffer);
      long endTs = System.currentTimeMillis();

      LOG.debug("request " + request + " with seqId=" + seqId + " build request use time "
        + (endTs - startTs));

      cf.addListener(new RequesterChannelFutureListener(seqId, request));
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
      LOG.debug("send request " + request + " with seqId " + seqId + "complete");
      if (!future.isSuccess()) {
        LOG.error("send " + seqId + " failed ", future.cause());
        future.cause().printStackTrace();
        requestFailed(seqId, request);
      }
    }
  }

  /**
   * RPC responses handler
   */
  public class Responser extends Thread {
    @Override
    public void run() {
      try {
        while (!stopped.get() && !Thread.interrupted()) {
          ByteBuf msg = msgQueue.take();
          int seqId = msg.readInt();

          // find the partition request context from cache
          Request request = seqIdToRequestMap.get(seqId);
          if (request == null) {
            continue;
          }

          TransportMethod method = request.getType();

          LOG.debug("response handler, seqid = " + seqId + ", method = " + method + ", ts = "
              + System.currentTimeMillis());

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
        }
      } catch (InterruptedException ie) {
        LOG.warn(Thread.currentThread().getName() + " is interruptted");
      } catch (Exception x) {
        LOG.fatal("hanlder rpc response failed ", x);
        PSAgentContext.get().getPsAgent().error("hanlder rpc response failed " + x.getMessage());
      }
    }
    
    @SuppressWarnings("unchecked")
    private void handleGetUDFResponse(ByteBuf buf, int seqId, GetUDFRequest request) {
      GetUDFResponse response = new GetUDFResponse();
      response.deserialize(buf);

      if (response.getResponseType() == ResponseType.SUCCESS) {
        FutureResult<PartitionGetResult> future = requestToResultMap.remove(request);
        if (future != null) {
          future.set(response.getPartResult());
        }
        requestSuccess(seqId, request);
      } else if (response.getResponseType() == ResponseType.FATAL) {
        String errorMsg = "get udf fatal error happened " + response.getDetail();
        LOG.fatal(errorMsg);
        PSAgentContext.get().getPsAgent().error(errorMsg);
      } else {
        LOG.error("get udf error happened " + response.getDetail() + ", retry later");
        requestFailed(seqId, request);
      }
    }

    @SuppressWarnings("unchecked")
    private void handleUpdaterResponse(ByteBuf buf, int seqId, UpdaterRequest request) {
      UpdaterResponse response = new UpdaterResponse();
      response.deserialize(buf);

      if (response.getResponseType() == ResponseType.SUCCESS) {
        FutureResult<VoidResult> future = requestToResultMap.remove(request);
        if (future != null) {
          future.set(new VoidResult(com.tencent.angel.psagent.matrix.ResponseType.SUCCESS));
        }
        requestSuccess(seqId, request);
      } else if (response.getResponseType() == ResponseType.FATAL) {
        String errorMsg = "updater fatal error happened " + response.getDetail();
        LOG.fatal(errorMsg);
        PSAgentContext.get().getPsAgent().error(errorMsg);
      } else {
        LOG.error("updater error happened " + response.getDetail() + ", retry later");
        requestFailed(seqId, request);
      }
    }

    @SuppressWarnings("unchecked")
    private void handlePutPartUpdateResponse(ByteBuf buf, int seqId,
        PutPartitionUpdateRequest request) {
      PutPartitionUpdateResponse response = new PutPartitionUpdateResponse();
      response.deserialize(buf);

      if (response.getResponseType() == ResponseType.SUCCESS) {
        FutureResult<VoidResult> future = requestToResultMap.remove(request);
        if (future != null) {
          future.set(new VoidResult(com.tencent.angel.psagent.matrix.ResponseType.SUCCESS));
        }
        requestSuccess(seqId, request);
      } else if (response.getResponseType() == ResponseType.FATAL) {
        String errorMsg = "put partition update fatal error happened " + response.getDetail();
        LOG.fatal(errorMsg);
        PSAgentContext.get().getPsAgent().error(errorMsg);
      } else {
        LOG.error("put partupdate error happened " + response.getDetail() + ", retry later");
        requestFailed(seqId, request);
      }
    }

    @SuppressWarnings("unchecked")
    private void handleGetClocksResponse(ByteBuf buf, int seqId, GetClocksRequest request) {
      GetClocksResponse response = new GetClocksResponse();
      response.deserialize(buf);
      if (response.getResponseType() == ResponseType.SUCCESS) {
        Map<PartitionKey, Integer> partitionClocks = response.getClocks();
        FutureResult<Map<PartitionKey, Integer>> future = requestToResultMap.remove(request);
        if (future != null) {
          future.set(partitionClocks);
        }

        requestSuccess(seqId, request);
      } else if (response.getResponseType() == ResponseType.FATAL) {
        String errorMsg = "get clocks fatal error happened " + response.getDetail();
        LOG.fatal(errorMsg);
        PSAgentContext.get().getPsAgent().error(errorMsg);
      } else {
        LOG.error("get clocks error happened " + response.getDetail() + ", retry later");
        requestFailed(seqId, request);
      }
    }

    @SuppressWarnings("unchecked")
    private void handleGetPartitionResponse(ByteBuf buf, int seqId, GetPartitionRequest request) {
      GetPartitionResponse response = new GetPartitionResponse();
      response.deserialize(buf);
      if (response.getResponseType() == ResponseType.SUCCESS) {
        updateMatrixCache(request.getPartKey(), response.getPartition());
        FutureResult<ServerPartition> future = requestToResultMap.remove(request);
        if (future != null) {
          future.set(response.getPartition());
        }

        requestSuccess(seqId, request);
      } else if (response.getResponseType() == ResponseType.NOTREADY) {
        requestNotReady(seqId, request);
      } else if (response.getResponseType() == ResponseType.FATAL) {
        String errorMsg = "get row split fatal error happened " + response.getDetail();
        LOG.fatal(errorMsg);
        PSAgentContext.get().getPsAgent().error(errorMsg);
      } else {
        LOG.error("get row split error happened " + response.getDetail() + ", retry later");
        requestFailed(seqId, request);
      }
    }

    @SuppressWarnings("unchecked")
    private void handleGetRowsSplitResponse(ByteBuf buf, int seqId, GetRowsSplitRequest request) {
      GetRowsSplitResponse response = new GetRowsSplitResponse();
      response.deserialize(buf);
      if (response.getResponseType() == ResponseType.SUCCESS) {
        updateMatrixCache(request.getPartKey(), response.getRowsSplit());
        FutureResult<List<ServerRow>> future = requestToResultMap.remove(request);
        if (future != null) {
          future.set(response.getRowsSplit());
        }

        requestSuccess(seqId, request);
      } else if (response.getResponseType() == ResponseType.NOTREADY) {
        requestNotReady(seqId, request);
      } else if (response.getResponseType() == ResponseType.FATAL) {
        String errorMsg = "get row split fatal error happened " + response.getDetail();
        LOG.fatal(errorMsg);
        PSAgentContext.get().getPsAgent().error(errorMsg);
      } else {
        LOG.error("get row split error happened " + response.getDetail() + ", retry later");
        requestFailed(seqId, request);
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

      if (response.getResponseType() == ResponseType.SUCCESS) {
        updateMatrixCache(request.getPartKey(), response.getRowSplit());
        FutureResult<ServerRow> future = requestToResultMap.remove(request);
        if (future != null) {
          future.set(response.getRowSplit());
        }

        requestSuccess(seqId, request);
      } else if (response.getResponseType() == ResponseType.NOTREADY) {
        requestNotReady(seqId, request);
      } else if (response.getResponseType() == ResponseType.FATAL) {
        String errorMsg = "get row split fatal error happened " + response.getDetail();
        LOG.fatal(errorMsg);
        PSAgentContext.get().getPsAgent().error(errorMsg);
      } else {
        LOG.error("get row split error happened " + response.getDetail() + ", retry later");
        requestFailed(seqId, request);
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
        PSAgentContext.get().getMatrixPartitionRouter().getPSId(partitionUpdaterParam.getPartKey());

    UpdaterRequest request =
        new UpdaterRequest(serverId, partitionUpdaterParam.getPartKey(), updateFunc.getClass()
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
        PSAgentContext.get().getMatrixPartitionRouter().getPSId(partitionGetParam.getPartKey());

    GetUDFRequest request =
        new GetUDFRequest(serverId, partitionGetParam.getPartKey(),
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
