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
  private ExecutorService requestThreadPool;

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

  private final boolean disableRouterCache;

  private final int partReplicNum;

  private volatile PSAgentPSFailedReporter reporter;

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

    partReplicNum = conf.getInt(AngelConf.ANGEL_PS_HA_REPLICATION_NUMBER,
      AngelConf.DEFAULT_ANGEL_PS_HA_REPLICATION_NUMBER);
    disableRouterCache = partReplicNum > 1;

    channelManager = null;
  }

  private void init() {
    Configuration conf = PSAgentContext.get().getConf();
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

    reporter = new PSAgentPSFailedReporter(PSAgentContext.get());
    reporter.init();
    reporter.start();

    requestDispacher = new RequestDispatcher();
    requestDispacher.start();

    responseDispatcher = new ResponseDispatcher();
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

      if(reporter != null) {
        reporter.stop();
        reporter = null;
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
    dispatchMessageQueue.put(new DispatcherEvent(EventType.PERIOD_CHECK));
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
      psIds = PSAgentContext.get().getLocationManager().getPsIds();
      lastChosenPutServerIndex = new Random().nextInt(psIds.length);
      lastChosenGetServerIndex = new Random().nextInt(psIds.length);

      totalBytesInFlightPut = new AtomicInteger(0);
      totalBytesInFlightGet = new AtomicInteger(0);
      totalRequestNumInFlightAtomic = new AtomicInteger(0);
      maxBytesInFlight = PSAgentContext.get().getMaxBytesInFlight();
      int serverNum =
          conf.getInt(AngelConf.ANGEL_PS_NUMBER,
              AngelConf.DEFAULT_ANGEL_PS_NUMBER);

      maxReqNumInFlightPerServer =
          conf.getInt(AngelConf.ANGEL_MATRIXTRANSFER_MAX_REQUESTNUM_PERSERVER,
              AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_MAX_REQUESTNUM_PERSERVER);

      reqNumInFlightCounters = new HashMap<>();
      for (int i = 0; i < psIds.length; i++) {
        reqNumInFlightCounters.put(psIds[i], 0);
      }

      int maxReqNumInFlight =
          conf.getInt(AngelConf.ANGEL_MATRIXTRANSFER_MAX_REQUESTNUM,
              AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_MAX);

      if (maxReqNumInFlight > serverNum * maxReqNumInFlightPerServer) {
        maxReqNumInFlight = serverNum * maxReqNumInFlightPerServer;
      }
      this.maxReqNumInFlight = maxReqNumInFlight;

      totalRequestNumInFlight = 0;

      failedPutCache = new LinkedList<>();
      failedGetCache = new LinkedList<>();
      schedulableFailedPutCache = new LinkedBlockingQueue<>();
      schedulableFailedGetCache = new LinkedBlockingQueue<>();
      waitGetList = new LinkedList<>();
      serverFailedStatics = new HashMap<>();

      refreshThreshold =
          conf.getInt(AngelConf.ANGEL_REFRESH_SERVERLOCATION_THRESHOLD,
              AngelConf.DEFAULT_ANGEL_REFRESH_SERVERLOCATION_THRESHOLD);

      refreshingServerSet = new HashSet<>();

      requestTimeOut =
          conf.getInt(AngelConf.ANGEL_MATRIXTRANSFER_REQUEST_TIMEOUT_MS,
              AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_REQUEST_TIMEOUT_MS);

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
              serverFailedStatics.put(request.getContext().getServerId(), 0);
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
              if(request instanceof PartitionRequest) {
                failedGetCache.add(request);
                refreshConn(request);
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
              serverFailedStatics.put(request.getContext().getServerId(), 0);
              updatePutFlowControl(request, -1);
              putDataSplit();
              break;
            }

            case PUT_FAILED: {
              Request request = ((RequestDispatchEvent) event).getRequest();
              request.getContext().setFailedTs(System.currentTimeMillis());
              updatePutFlowControl(request, -1);

              if(request instanceof PartitionRequest) {
                failedPutCache.add(request);
                refreshConn(request);
              }
              putDataSplit();
              break;
            }

            case REFRESH_SERVER_LOCATION_SUCCESS: {
              RefreshServerLocationEvent refreshEvent = (RefreshServerLocationEvent) event;
              ParameterServerId serverId = refreshEvent.getServerId();
              LOG.info("refresh location for server " + serverId + " success ");
              if(refreshEvent.isUpdated()) {
                closeChannelForServer(serverId);
                reDispatchEventForServer(serverId);
              }
              removeRefreshingServer(serverId);
              dispatchTransportEvent(serverId);
              break;
            }

            case REFRESH_SERVER_LOCATION_FAILED: {
              ParameterServerId serverId = ((RefreshServerLocationEvent) event).getServerId();
              removeRefreshingServer(serverId);
              LOG.fatal("get server location from master failed");
              PSAgentContext.get().getPsAgent().error("get server location from master failed");
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

    private void refreshConn(Request request) {
      if(partReplicNum == 1) {
        Integer counter = serverFailedStatics.get(request.getContext().getServerId());
        if (counter == null) {
          serverFailedStatics.put(request.getContext().getServerId(), 1);
        } else {
          serverFailedStatics.put(request.getContext().getServerId(), counter + 1);
        }

        if (serverFailedStatics.get(request.getContext().getServerId()) >= refreshThreshold) {
          LOG.info("PS " + request.getContext().getServerId() + " failed time "
            + serverFailedStatics.get(request.getContext().getServerId()) + " over " + refreshThreshold);
          addRefreshingServer(request.getContext().getServerId());
          closeChannelForServer(request.getContext().getServerId());
          refreshServerLocation(request.getContext().getServerId());
          serverFailedStatics.put(request.getContext().getServerId(), 0);
        }
      }
    }

    private void updateGetFlowControl(Request request, int factor) {
      updateGetBytesInFlight(request, factor);
      updateReqNumInFlight(request.getContext().getServerId(), factor);
    }

    private void updatePutFlowControl(Request request, int factor) {
      updatePutBytesInFlight(request, factor);
      updateReqNumInFlight(request.getContext().getServerId(), factor);
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
        if (isRefreshing(item.getContext().getServerId())) {
          return 1;
        }

        if (!checkIsOverGetFlowControlLimit(item) && !checkIsOverReqNumLimit(item.getContext().getServerId())) {
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
        if (isRefreshing(item.getContext().getServerId())) {
          return 1;
        }

        if (!checkIsOverPutFlowControlLimit(item) && !checkIsOverReqNumLimit(item.getContext().getServerId())) {
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
      requestThreadPool.execute(new Requester(item, seqId));
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
        LOG.debug("infight request seqId=" + entry.getKey() + ", request context=" + entry.getValue()
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
        if ((serverId == null || item.getContext().getServerId() == serverId)
            && !isRefreshing(item.getContext().getServerId())
            && (ts - item.getContext().getFailedTs() >= retryIntervalMs)) {
          schedulableFailedPutCache.add(item);
          iter.remove();
        }
      }
      putDataSplit();

      iter = failedGetCache.iterator();
      while (iter.hasNext()) {
        Request item = iter.next();
        if ((serverId == null || item.getContext().getServerId() == serverId)
            && !isRefreshing(item.getContext().getServerId())
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
          getItemQueues.get(item.getContext().getServerId()).add(item);
          iter.remove();
        } catch (Exception x) {
          LOG.warn("add " + item + " to getItemQueues falied, ", x);
        }
      }
    }

    private void reDispatchEventForServer(ParameterServerId serverId) {
      LOG.debug("redispatch all events for server " + serverId);
      Iterator<Entry<Integer, Request>> iter = seqIdToRequestMap.entrySet().iterator();
      Entry<Integer, Request> entry = null;
      while(iter.hasNext()) {
        entry = iter.next();
        if(entry.getValue().getContext().getServerId().equals(serverId)) {
          reDispatchRequest(entry.getValue());
          LOG.debug("remove request " + entry.getValue() + " from  seqIdToRequestMap");
          iter.remove();
        }
      }
    }

    private void reDispatchRequest(Request request) {
      returnChannel(request);
      switch (request.getType()) {
        case GET_PART:
        case GET_ROWSPLIT:
        case GET_ROWSSPLIT:
        case GET_CLOCKS:
        case GET_UDF:
          schedulableFailedGetCache.add(request);
          break;

        case PUT_PART:
        case PUT_PARTUPDATE:
        case UPDATER:
          schedulableFailedPutCache.add(request);
          break;

        default:
          LOG.error("unvalid request " + request);
          break;
      }
    }

    private void removeTimeOutRequestItem() {
      int removeNum = 0;
      try {
        for (Entry<Integer, Request> entry : seqIdToRequestMap.entrySet()) {
          Request item = entry.getValue();
          item.getContext().addWaitTimeTicks(checkPeriodMS * 10);
          LOG.debug("request seqId=" + entry.getKey() + " wait time="
              + item.getContext().getWaitTimeTicks());
          if (item.getContext().getWaitTimeTicks() > requestTimeOut) {
            item = seqIdToRequestMap.get(entry.getKey());
            if (item != null) {
              LOG.info("remove timeout request seqId=" + entry.getKey() + ", request=" + entry.getValue());
              removeNum++;
              requestFailed(entry.getKey(), item, ResponseType.TIMEOUT, "request timeout");
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
              requestFailed(entry.getKey(), item, ResponseType.NETWORK_ERROR, "channel is closed");
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

  private void requestFailed(int seqId, Request request, ResponseType failedType, String errorLog) {
    if(reporter == null) {
      return;
    }

    seqIdToRequestMap.remove(seqId);
    returnChannel(request);
    if(failedType == ResponseType.NETWORK_ERROR || failedType == ResponseType.TIMEOUT && request.getContext().getActualServerId() != null) {
      reporter.psFailed(new PSLocation(request.getContext().getActualServerId(), request.getContext().getLocation()));
    }

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

  private void getRequestFailed(Request request, ResponseType failedType, String errorLog) {
    try {
      dispatchMessageQueue.add(new RequestFailedEvent(EventType.GET_FAILED, request, failedType, errorLog));
    } catch (Exception e) {
      LOG.error("add GET_FAILED event for request " + request + " failed, ", e);
    }
  }

  private void putRequestFailed(Request request, ResponseType failedType, String errorLog) {
    try {
      dispatchMessageQueue.add(new RequestFailedEvent(EventType.PUT_FAILED, request, failedType, errorLog));
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
            LOG.info("Get PS " + serverId + " location = " + location);
            if(location != null) {
              Location oldLocation = PSAgentContext.get().getLocationManager().getPsLocation(serverId);
              PSAgentContext.get().getLocationManager().setPsLocation(serverId, location);
              if(oldLocation != null && location.equals(oldLocation)) {
                refreshServerLocationSuccess(serverId, false);
              } else {
                refreshServerLocationSuccess(serverId, true);
              }
              return;
            }           
          }
        } catch (Exception x) {
          refreshServerLocationFailed(serverId);
        }
      }
    };

    psLocRefresher.setName("ps-location-getter");
    psLocRefresher.start();
  }

  private void refreshServerLocationSuccess(ParameterServerId serverId, boolean isUpdated) {
    dispatchMessageQueue.add(new RefreshServerLocationEvent(
        EventType.REFRESH_SERVER_LOCATION_SUCCESS, serverId, isUpdated));
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
    Location loc = PSAgentContext.get().getLocationManager().getPsLocation(serverId);
    if (loc == null) {
      return;
    }

    channelManager.removeChannelPool(loc);
  }

  private GenericObjectPool<Channel> getChannelPool(Location loc) throws InterruptedException {
    return channelManager.getOrCreateChannelPool(new Location(loc.getIp(), loc.getPort() + 1), PSAgentContext
            .get()
            .getConf()
            .getInt(AngelConf.ANGEL_WORKER_TASK_NUMBER,
                AngelConf.DEFAULT_ANGEL_WORKER_TASK_NUMBER));
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
      PSAgentContext.get().getLocationManager().setPsLocation(serverId, location);
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

      // allocate the bytebuf
      ByteBuf buffer = ByteBufUtils.newByteBuf(request.bufferLen(), useDirectBuffer);
      buffer.writeInt(seqId);
      buffer.writeInt(request.getType().getMethodId());
      request.serialize(buffer);
      if(!(request instanceof GetClocksRequest)) {
        LOG.debug("serialize request use time=" + (System.currentTimeMillis() - startTs));
      }

      startTs = System.currentTimeMillis();
      // check the location of server is ready, if not, we should wait
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
      request.getContext().setActualServerId(serverId);
      request.getContext().setLocation(loc);

      if (loc == null) {
        LOG.error("request " + request + " server " + request.getContext().getServerId() + " location is null");
        if (loc == null) {
          requestFailed(seqId, request, ResponseType.SERVER_NOT_READY, "location is null");
          return;
        }
      }

      if(!(request instanceof GetClocksRequest) && LOG.isDebugEnabled()) {
        LOG.debug("request " + request + " with seqId=" + seqId + " get location use time " + (System.currentTimeMillis() - startTs));
      }

      // get a channel to server from pool
      Channel channel = null;
      GenericObjectPool<Channel> pool = null;

      startTs = System.currentTimeMillis();
      try {
        pool = getChannelPool(loc);
        if(!(request instanceof GetClocksRequest) && LOG.isDebugEnabled()) {
          LOG.debug("request " + request + " with seqId=" + seqId + " pool=" + pool.getNumActive());
        }
        channel = pool.borrowObject();
        if(!(request instanceof GetClocksRequest) && LOG.isDebugEnabled()) {
          LOG.debug("request " + request + " with seqId=" + seqId + " wait for channel use time " + (
            System.currentTimeMillis() - startTs));
        }

        LOG.debug("request " + request + " with seqId=" + seqId + " wait for channel use time " + (
          System.currentTimeMillis() - startTs));

        // if channel is not valid, it means maybe the connections to the server are closed
        if (!channel.isActive() || !channel.isOpen()) {
          LOG.error("channel " + channel + " is not active");
          // channelManager.removeChannelPool(loc);
          requestFailed(seqId, request, ResponseType.NETWORK_ERROR, "channel get from pool is not active or closed");
          return;
        }
      } catch (Exception x) {
        if(!stopped.get()) {
          LOG.error("send request " + request + " get channel failed ", x);
          requestFailed(seqId, request, ResponseType.NETWORK_ERROR, "connect to server failed");
        }
        return;
      }

      request.getContext().setChannelPool(pool);
      request.getContext().setChannel(channel);

      startTs = System.currentTimeMillis();
      ChannelFuture cf = channel.writeAndFlush(buffer);

      if(!(request instanceof GetClocksRequest) && LOG.isDebugEnabled()) {
        LOG.debug("request " + request + " with seqId=" + seqId + " send use time " + (System.currentTimeMillis() - startTs));
      }

      cf.addListener(new RequesterChannelFutureListener(seqId, request));
    }
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
      LOG.debug("send request " + request + " with seqId=" + seqId + " complete");
      if (!future.isSuccess()) {
        LOG.error("send " + seqId + " failed ", future.cause());
        requestFailed(seqId, request, ResponseType.NETWORK_ERROR, "send request failed " + future.cause().toString());
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
      try {
        int seqId = msg.readInt();

        // find the partition request context from cache
        Request request = seqIdToRequestMap.get(seqId);
        long startTs = System.currentTimeMillis();
        if (request == null) {
          return;
        }

        TransportMethod method = request.getType();

        if(!(request instanceof GetClocksRequest) && LOG.isDebugEnabled()) {
          LOG.info("response handler, seqId=" + seqId + ", method=" + method + ", ts=" + System
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

        if (!(request instanceof GetClocksRequest) && LOG.isDebugEnabled()) {
          LOG.debug(
            "handle response of request " + request + " use time=" + (System.currentTimeMillis()
              - startTs));
        }
      } catch (InterruptedException ie) {
        LOG.warn(Thread.currentThread().getName() + " is interruptted");
      } catch (Throwable x) {
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
      } else if (response.getResponseType() == ResponseType.SERVER_HANDLE_FATAL) {
        String errorMsg = "get udf fatal error happened " + response.getDetail();
        LOG.fatal(errorMsg);
        PSAgentContext.get().getPsAgent().error(errorMsg);
      } else {
        LOG.error("get udf error happened " + response.getDetail() + ", retry later");
        requestFailed(seqId, request, ResponseType.SERVER_HANDLE_FAILED, response.getDetail());
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
      } else if (response.getResponseType() == ResponseType.SERVER_HANDLE_FATAL) {
        String errorMsg = "updater fatal error happened " + response.getDetail();
        LOG.fatal(errorMsg);
        PSAgentContext.get().getPsAgent().error(errorMsg);
      } else {
        LOG.error("updater error happened " + response.getDetail() + ", retry later");
        requestFailed(seqId, request, ResponseType.SERVER_HANDLE_FAILED, response.getDetail());
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
      } else if (response.getResponseType() == ResponseType.SERVER_HANDLE_FATAL) {
        String errorMsg = "put partition update fatal error happened " + response.getDetail();
        LOG.fatal(errorMsg);
        PSAgentContext.get().getPsAgent().error(errorMsg);
      } else {
        LOG.error("put partupdate error happened " + response.getDetail() + ", retry later");
        requestFailed(seqId, request, ResponseType.SERVER_HANDLE_FAILED, response.getDetail());
      }
    }

    @SuppressWarnings("unchecked")
    private void handleGetClocksResponse(ByteBuf buf, int seqId, GetClocksRequest request) {
      GetClocksResponse response = new GetClocksResponse();
      response.deserialize(buf);

      FutureResult<GetClocksResponse> future = requestToResultMap.remove(request);
      if (future != null) {
        future.set(response);
      }

      if (response.getResponseType() == ResponseType.SUCCESS) {
        requestSuccess(seqId, request);
      } else if (response.getResponseType() == ResponseType.SERVER_HANDLE_FATAL) {
        String errorMsg = "get clocks fatal error happened " + response.getDetail();
        LOG.fatal(errorMsg);
        PSAgentContext.get().getPsAgent().error(errorMsg);
      } else {
        LOG.error("get clocks error happened " + response.getDetail() + ", retry later");
        requestFailed(seqId, request, ResponseType.SERVER_HANDLE_FAILED, response.getDetail());
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
      } else if (response.getResponseType() == ResponseType.CLOCK_NOTREADY) {
        requestNotReady(seqId, request);
      } else if (response.getResponseType() == ResponseType.SERVER_HANDLE_FATAL) {
        String errorMsg = "get row split fatal error happened " + response.getDetail();
        LOG.fatal(errorMsg);
        PSAgentContext.get().getPsAgent().error(errorMsg);
      } else {
        LOG.error("get row split error happened " + response.getDetail() + ", retry later");
        requestFailed(seqId, request, ResponseType.SERVER_HANDLE_FAILED, response.getDetail());
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
      } else if (response.getResponseType() == ResponseType.CLOCK_NOTREADY) {
        requestNotReady(seqId, request);
      } else if (response.getResponseType() == ResponseType.SERVER_HANDLE_FATAL) {
        String errorMsg = "get row split fatal error happened " + response.getDetail();
        LOG.fatal(errorMsg);
        PSAgentContext.get().getPsAgent().error(errorMsg);
      } else {
        LOG.error("get row split error happened " + response.getDetail() + ", retry later");
        requestFailed(seqId, request, ResponseType.SERVER_HANDLE_FAILED, response.getDetail());
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
      } else if (response.getResponseType() == ResponseType.CLOCK_NOTREADY) {
        requestNotReady(seqId, request);
      } else if (response.getResponseType() == ResponseType.SERVER_HANDLE_FATAL) {
        String errorMsg = "get row split fatal error happened " + response.getDetail();
        LOG.fatal(errorMsg);
        PSAgentContext.get().getPsAgent().error(errorMsg);
      } else {
        LOG.error("get row split error happened " + response.getDetail() + ", retry later");
        requestFailed(seqId, request, ResponseType.SERVER_HANDLE_FAILED, response.getDetail());
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
