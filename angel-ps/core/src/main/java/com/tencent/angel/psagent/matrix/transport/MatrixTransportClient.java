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
import com.tencent.angel.ps.metrics.AngelWorkerMetricsService;
import com.tencent.angel.ps.server.data.PSLocation;
import com.tencent.angel.ps.server.data.ServerState;
import com.tencent.angel.ps.server.data.TransportMethod;
import com.tencent.angel.ps.server.data.profiling.ClientTimes;
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
import io.netty.handler.timeout.IdleStateHandler;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteOrder;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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

  // NOTE: the historical {@code msgQueue} + {@code ResponseDispatcher} pair has been removed
  // because no production code path ever put anything into the queue -- responses arrive on
  // the netty IO thread and are submitted directly to {@code responseThreadPool} via
  // {@link #handleResponse}. The dispatcher thread was permanently blocked on take(), wasting
  // one platform thread and confusing operators who saw "RPCResponseDispatcher" in jstack and
  // assumed it was doing work.

  /**
   * stop the all services in the rpc client
   */
  private final AtomicBoolean stopped;

  /**
   * event dispatcher for matrix transport client
   */
  private RequestDispatcher requestDispacher;

  /**
   * Event queue for the request dispatcher.
   *
   * <p>Bounded to prevent unbounded producer bursts (timeouts/retries/channel-close storms)
   * from accumulating an arbitrarily large in-memory backlog.</p>
   */
  private final LinkedBlockingQueue<DispatcherEvent> dispatchMessageQueue;

  /** Configured capacity of {@link #dispatchMessageQueue}. */
  private final int dispatchQueueCapacity;


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

  // Historical "RPCResponser" thread (ResponseDispatcher) removed -- see note above msgQueue.

  // {@code getUseTimes} field removed: never written to anywhere in the codebase, only an
  // empty getter remained. If per-request timing is needed, prefer {@link AngelWorkerMetricsService}.

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
   * Shared scheduler for the lightweight background tasks (PS heartbeat to gray PSs and
   * failed-PS location refresh). Replaces the previous pair of bare {@link Thread} subclasses
   * that each consumed an OS thread for what is essentially a periodic 1-task job; the
   * scheduler also gives us clean cancellation via {@link java.util.concurrent.ScheduledFuture#cancel}
   * instead of relying on {@code interrupt()} during {@link #stop()}.
   */
  private volatile java.util.concurrent.ScheduledExecutorService bgScheduler;

  /**
   * Dedicated executor used to time-bound master RPC calls (see {@link #callMasterWithTimeout}).
   * Kept physically separate from {@code bgScheduler} and {@code hbThreadPool} so that a
   * synchronously stuck master RPC consumes a thread from a small isolated pool rather than
   * from a pool that any other recovery path depends on. Sized at 2 because the only callers
   * are the single-threaded PSLocRefresher tick and the per-PS heartbeat probes — concurrent
   * outstanding master RPCs above 2 indicate the master itself is sick, in which case we
   * want the extra calls to drop fast rather than queue.
   */
  private volatile java.util.concurrent.ExecutorService masterRpcExecutor;

  /** Configured per-call timeout (ms) for every master RPC issued by this client. */
  private volatile int masterRpcTimeoutMs;

  /** Handle to the periodic PSHeartbeat task; cancelled in {@link #stop()}. */
  private volatile java.util.concurrent.ScheduledFuture<?> psHeartbeater;

  /** Handle to the periodic PSLocRefresher task; cancelled in {@link #stop()}. */
  private volatile java.util.concurrent.ScheduledFuture<?> psLocRefresher;

  /** Tracks the last time PSLocRefresher successfully spoke with the master. */
  private volatile long lastMasterHbTs = System.currentTimeMillis();

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
   * Set of PS server-id integers whose STUCK_PS state should be force-bumped at every
   * {@code checkChannelContext} tick — purely a fault-injection hook for regression
   * testing. Populated from {@link AngelConf#ANGEL_PSAGENT_FAULT_INJECT_STUCK_PS_IDS} in
   * {@link #init()}; empty by default. See that AngelConf entry's javadoc for the
   * rationale and exact semantics.
   */
  private volatile java.util.Set<Integer> faultInjectStuckPsIds = java.util.Collections.emptySet();

  // {@code psLocToNoActiveCounterMap} removed: declared but never written/read anywhere.
  // The active counter we actually use is GetChannelContext.notactiveCounter (per-PS).

  /**
   * PS location to last "channel inactive" log timestamp (ms). Used to throttle the inactive
   * channel error logs to at most one per second per PS, so a burst of N concurrent in-flight
   * RPCs hitting the same dead long-lived connection no longer produces N near-identical
   * IOException stack traces in the driver log.
   */
  private final ConcurrentHashMap<PSLocation, Long> psLocToLastInactiveLogTsMap =
      new ConcurrentHashMap<>();
  private static final long INACTIVE_LOG_THROTTLE_MS = 1000L;

  /**
   * Per-{@link ResponseType} back-off + try-limit policy. Constructed in {@link #init()}
   * from the AngelConf values and shared with the {@code RequestDispatcher}. Extracted to a
   * dedicated class for testability — see {@link RetryPolicy}.
   */
  private volatile RetryPolicy retryPolicy;

  private final ConcurrentHashMap<TransportMethod, Handler> responseHanders;

  private volatile AngelWorkerMetricsService metricsService;

  public AngelWorkerMetricsService getMetricsService() {
    return metricsService;
  }

  /**
   * Create a new MatrixTransportClient.
   */
  @SuppressWarnings("rawtypes")
  public MatrixTransportClient() {
    seqIdToRequestMap = new ConcurrentHashMap<>();
    requestToResultMap = new ConcurrentHashMap<>();

    Configuration conf = PSAgentContext.get().getConf();
    dispatchQueueCapacity = conf.getInt(
        AngelConf.ANGEL_MATRIXTRANSFER_DISPATCH_EVENT_QUEUE_CAPACITY,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_DISPATCH_EVENT_QUEUE_CAPACITY);
    dispatchMessageQueue = new LinkedBlockingQueue<>(dispatchQueueCapacity);
    getItemQueues = new ConcurrentHashMap<>();
    putItemQueues = new ConcurrentHashMap<>();
    responseHanders = new ConcurrentHashMap<>();

    stopped = new AtomicBoolean(false);

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

    // Parse the fault-injection list. Stored as a comma-separated list of integers; we
    // tolerate whitespace and ignore unparseable tokens with a warning so a typo in a
    // single id doesn't disable the rest of the list.
    String injectIds = conf.get(AngelConf.ANGEL_PSAGENT_FAULT_INJECT_STUCK_PS_IDS,
        AngelConf.DEFAULT_ANGEL_PSAGENT_FAULT_INJECT_STUCK_PS_IDS);
    if (injectIds != null && !injectIds.trim().isEmpty()) {
      java.util.HashSet<Integer> ids = new java.util.HashSet<>();
      for (String tok : injectIds.split(",")) {
        String t = tok.trim();
        if (t.isEmpty()) continue;
        try {
          ids.add(Integer.parseInt(t));
        } catch (NumberFormatException nfe) {
          LOG.warn("Ignoring unparseable PS id token in "
              + AngelConf.ANGEL_PSAGENT_FAULT_INJECT_STUCK_PS_IDS + ": '" + t + "'");
        }
      }
      if (!ids.isEmpty()) {
        faultInjectStuckPsIds = java.util.Collections.unmodifiableSet(ids);
        LOG.warn("STUCK_PS fault injection ENABLED for PS ids " + ids
            + ". This must NEVER be set in production.");
      }
    }

    channelManager = null;
    rpcContext = new RPCContext();
  }

  private void init() {
    Configuration conf = PSAgentContext.get().getConf();

    rpcContext.init(conf, PSAgentContext.get().getLocationManager().getPsIds());

    metricsService = new AngelWorkerMetricsService(conf);
    metricsService.init();

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

    // Use a bounded queue + CallerRunsPolicy on both pools so that bursts of pending requests
    // cannot pile up unbounded direct memory (which would OOM the JVM). CallerRunsPolicy
    // produces natural back-pressure: a saturated requester pool runs the next submit on
    // the dispatcher thread (slowing further submits), while a saturated responder pool
    // makes the netty IO thread itself deserialize the current message (slowing the TCP
    // receive window from the server).
    int requesterPoolSize = conf.getInt(
        AngelConf.ANGEL_MATRIXTRANSFER_CLIENT_REQUESTER_POOL_SIZE,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_CLIENT_REQUESTER_POOL_SIZE);
    int requesterQueueCap = conf.getInt(
        AngelConf.ANGEL_MATRIXTRANSFER_CLIENT_REQUESTER_QUEUE_CAPACITY,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_CLIENT_REQUESTER_QUEUE_CAPACITY);
    requestThreadPool = new java.util.concurrent.ThreadPoolExecutor(
        requesterPoolSize, requesterPoolSize, 60L, java.util.concurrent.TimeUnit.SECONDS,
        new java.util.concurrent.LinkedBlockingQueue<Runnable>(requesterQueueCap),
        new AngelThreadFactory("RPCRequest"),
        new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());
    LOG.info("[PSAGENT-THREAD-POOL] RPCRequest executor: threadNamePrefix=RPCRequest"
        + ", poolSize=" + requesterPoolSize
        + ", queueCap=" + requesterQueueCap
        + ", rejectPolicy=CallerRunsPolicy");

    int responserPoolSize = conf.getInt(

        AngelConf.ANGEL_MATRIXTRANSFER_CLIENT_RESPONSER_POOL_SIZE,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_CLIENT_RESPONSER_POOL_SIZE);
    int responserQueueCap = conf.getInt(
        AngelConf.ANGEL_MATRIXTRANSFER_CLIENT_RESPONSER_QUEUE_CAPACITY,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_CLIENT_RESPONSER_QUEUE_CAPACITY);
    // Reject policy: replace the historical CallerRunsPolicy with a fail-fast handler that
    // converts a rejected response into an in-band SERVER_IS_BUSY failure on the matching
    // seqId. The original CallerRunsPolicy back-pressures by stalling the netty IO thread,
    // which also stalls every other channel sharing the same NioEventLoop -- a single slow
    // deserialize then cascades into widespread channelInactive events for unrelated PSes.
    // The new policy frees the IO thread within microseconds and lets the dispatcher's
    // standard retry back-off provide the back-pressure instead. Toggleable via
    // ANGEL_MATRIXTRANSFER_CLIENT_RESPONSER_FAIL_FAST_ON_REJECT for regression hatching.
    boolean responserFailFastOnReject = conf.getBoolean(
        AngelConf.ANGEL_MATRIXTRANSFER_CLIENT_RESPONSER_FAIL_FAST_ON_REJECT,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_CLIENT_RESPONSER_FAIL_FAST_ON_REJECT);
    responserRejectedPolicy = new ResponserRejectedPolicy(responserFailFastOnReject);
    responseThreadPool = new java.util.concurrent.ThreadPoolExecutor(
        responserPoolSize, responserPoolSize, 60L, java.util.concurrent.TimeUnit.SECONDS,
        new java.util.concurrent.LinkedBlockingQueue<Runnable>(responserQueueCap),
        new AngelThreadFactory("RPCResponser"),
        responserRejectedPolicy);
    LOG.info("[PSAGENT-THREAD-POOL] RPCResponse executor: threadNamePrefix=RPCResponser"
        + ", poolSize=" + responserPoolSize
        + ", queueCap=" + responserQueueCap
        + ", failFastOnReject=" + responserFailFastOnReject
        + ", rejectPolicy=" + (responserFailFastOnReject
        ? "ResponserRejectedPolicy(fail-fast)" : "ResponserRejectedPolicy(caller-runs)"));

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

    // Bounded heartbeat pool with DiscardPolicy: PS gray-list probes are best-effort, if the
    // queue is full we'd rather drop a probe than queue up unbounded work (the next tick will
    // try again). Was previously hardcoded to 8 with an unbounded LinkedBlockingQueue.
    int hbPoolSize = conf.getInt(
        AngelConf.ANGEL_MATRIXTRANSFER_CLIENT_HEARTBEAT_POOL_SIZE,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_CLIENT_HEARTBEAT_POOL_SIZE);
    int hbQueueCap = conf.getInt(
        AngelConf.ANGEL_MATRIXTRANSFER_CLIENT_HEARTBEAT_QUEUE_CAPACITY,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_CLIENT_HEARTBEAT_QUEUE_CAPACITY);
    hbThreadPool = new java.util.concurrent.ThreadPoolExecutor(
        hbPoolSize, hbPoolSize, 60L, java.util.concurrent.TimeUnit.SECONDS,
        new java.util.concurrent.LinkedBlockingQueue<Runnable>(hbQueueCap),
        new AngelThreadFactory("Heartbeat"),
        new java.util.concurrent.ThreadPoolExecutor.DiscardPolicy());

    bootstrap = new Bootstrap();
    channelManager = new ChannelManager2(bootstrap, poolParam);
    channelManager.setCheckIntervalMs(conf.getInt(
        AngelConf.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_POOL_CHECK_INTERVAL_MS,
        AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_POOL_CHECK_INTERVAL_MS));
    channelManager.initAndStart();

    // Build the per-ResponseType retry policy once and share between the client (for
    // isOverTryLimit) and the dispatcher (for next-retry timestamp computation).
    int requestTimeOutMs = conf.getInt(AngelConf.ANGEL_MATRIXTRANSFER_REQUEST_TIMEOUT_MS,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_REQUEST_TIMEOUT_MS);
    int retryBackoffMs = conf.getInt(AngelConf.ANGEL_MATRIXTRANSFER_RETRY_BACKOFF_WINDOW_MS,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_RETRY_BACKOFF_WINDOW_MS);
    retryPolicy = new RetryPolicy(retryIntervalMs, retryBackoffMs, requestTimeOutMs, maxTryNum);

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

    // Writer-idle interval (seconds) for the application-level keep-alive PING. When the
    // client has not WRITTEN for this long, ClientHeartbeatHandler pushes a 0-byte frame so
    // any NAT / cloud-LB / stateful firewall in the path keeps the conntrack entry alive.
    // Set to 0 to disable. See ANGEL_NETTY_MATRIXTRANSFER_CLIENT_WRITER_IDLE_SECONDS.
    final int writerIdleSeconds = conf.getInt(
        AngelConf.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_WRITER_IDLE_SECONDS,
        AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_WRITER_IDLE_SECONDS);

    LOG.info("[PSAGENT-THREAD-POOL] matrix-transport client: nettyEventLoopThreads="
        + nettyWorkerNum
        + ", requesterPoolSize=" + requesterPoolSize + ", requesterQueueCap=" + requesterQueueCap
        + ", responserPoolSize=" + responserPoolSize + ", responserQueueCap=" + responserQueueCap
        + ", heartbeatPoolSize=" + hbPoolSize + ", heartbeatQueueCap=" + hbQueueCap
        + ", ioRatio=" + ioRatio + ", channelType=" + channelType
        + ", writerIdleSeconds=" + writerIdleSeconds
        + ", checkPeriodMs=" + checkPeriodMS + ", retryIntervalMs=" + retryIntervalMs
        + ", maxTryNum=" + maxTryNum + ", hbTimeoutMs=" + hbTimeoutMS + ")");

    bootstrap.group(eventGroup).channel(channelClass).option(ChannelOption.SO_SNDBUF, sendBuffSize)

        .option(ChannelOption.SO_RCVBUF, recvBuffSize).option(ChannelOption.SO_KEEPALIVE, true)
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline pipeLine = ch.pipeline();
            pipeLine.addLast(new LengthFieldBasedFrameDecoder(maxMessageSize, 0, 4, 0, 4));
            pipeLine.addLast(new LengthFieldPrepender(4));
            // Application-level keep-alive: WRITER_IDLE only. Installed BEFORE the business
            // handler so that the IdleStateEvent reaches ClientHeartbeatHandler via
            // userEventTriggered. Both the handler and the IdleStateHandler are skipped
            // entirely when writerIdleSeconds <= 0 so existing deployments that prefer
            // server-side idle-close behaviour can opt out without code changes.
            if (writerIdleSeconds > 0) {
              pipeLine.addLast("idleStateHandler",
                  new IdleStateHandler(0, writerIdleSeconds, 0,
                      java.util.concurrent.TimeUnit.SECONDS));
              pipeLine.addLast("heartbeat", new ClientHeartbeatHandler());
            }
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

    // Replace the two bare Threads (PSHeartbeat / PSLocRefresher) with a small shared
    // scheduler. Each task is a single short-lived Runnable invoked periodically; using
    // fixed-delay semantics means we never overlap iterations even if one is slow.
    Configuration conf = PSAgentContext.get().getConf();
    int psHbIntervalMs = conf.getInt(
        AngelConf.ANGEL_PSAGENT_TO_PS_HEARTBEAT_INTERVAL_MS,
        AngelConf.DEFAULT_ANGEL_PSAGENT_TO_PS_HEARTBEAT_INTERVAL_MS);
    masterRpcTimeoutMs = conf.getInt(
        AngelConf.ANGEL_PSAGENT_MASTER_RPC_TIMEOUT_MS,
        AngelConf.DEFAULT_ANGEL_PSAGENT_MASTER_RPC_TIMEOUT_MS);
    // 2-thread bounded executor with DiscardPolicy: see field javadoc for sizing rationale.
    // 32-element bounded queue is far more than the legitimate working set (we never have
    // more than a handful of outstanding master RPCs at once); a full queue means master
    // is so sick that adding more is pointless, drop and let the next 5 s tick retry.
    masterRpcExecutor = new java.util.concurrent.ThreadPoolExecutor(
        2, 2, 60L, java.util.concurrent.TimeUnit.SECONDS,
        new java.util.concurrent.LinkedBlockingQueue<Runnable>(32),
        new AngelThreadFactory("master-rpc"),
        new java.util.concurrent.ThreadPoolExecutor.DiscardPolicy());
    bgScheduler = java.util.concurrent.Executors.newScheduledThreadPool(
        2, new AngelThreadFactory("psagent-bg"));
    LOG.info("[PSAGENT-THREAD-POOL] background executors: requestDispatcherThreads=1"
        + ", timerThreads=1"
        + ", bgSchedulerThreads=2"
        + ", masterRpcThreads=2"
        + ", masterRpcQueueCap=32"
        + ", psHeartbeatPeriodMs=" + psHbIntervalMs
        + ", psLocRefreshPeriodMs=5000"
        + ", masterRpcTimeoutMs=" + masterRpcTimeoutMs + ")");
    psHeartbeater = bgScheduler.scheduleWithFixedDelay(new Runnable() {

      @Override public void run() {
        if (!stopped.get()) {
          runPSHeartbeatOnce();
        }
      }
    }, psHbIntervalMs, psHbIntervalMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    // 5s for the location refresher matches the previous Thread.sleep(5000) cadence.
    psLocRefresher = bgScheduler.scheduleWithFixedDelay(new Runnable() {
      @Override public void run() {
        if (!stopped.get()) {
          runPSLocRefreshOnce();
        }
      }
    }, 5_000L, 5_000L, java.util.concurrent.TimeUnit.MILLISECONDS);

    requestDispacher = new RequestDispatcher();
    requestDispacher.setName("request-dispatcher");
    requestDispacher.start();

    // ResponseDispatcher no longer started: responses are submitted to responseThreadPool
    // directly from the netty IO thread via handleResponse (see MatrixTransportClientHandler).

    metricsService.start();

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

      // Cancel the lightweight background tasks and tear down their shared scheduler so
      // JVM shutdown isn't delayed by the residual sleep window of each periodic task.
      if (psHeartbeater != null) {
        psHeartbeater.cancel(false);
        psHeartbeater = null;
      }
      if (psLocRefresher != null) {
        psLocRefresher.cancel(false);
        psLocRefresher = null;
      }
      if (bgScheduler != null) {
        bgScheduler.shutdownNow();
        bgScheduler = null;
      }

      if (masterRpcExecutor != null) {
        masterRpcExecutor.shutdownNow();
        masterRpcExecutor = null;
      }

      if (requestDispacher != null) {
        requestDispacher.interrupt();
        requestDispacher = null;
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

      if (hbThreadPool != null) {
        hbThreadPool.shutdownNow();
      }

      if (metricsService != null) {
        metricsService.stop();
        metricsService = null;
      }
    }
  }

  private void periodCheck() throws InterruptedException {
    try {
      // PERIOD_CHECK is coalesced in RequestDispatcher; when queue is full we can safely
      // drop this tick and rely on the next one, avoiding producer-side blocking cascades.
      if (!dispatchMessageQueue.offer(new DispatcherEvent(EventType.PERIOD_CHECK))) {
        metricsService.incrementAtomicInteger(FailedMsg.PERIOD_CHECK);
        if (LOG.isDebugEnabled()) {
          LOG.debug("drop PERIOD_CHECK event because dispatch queue is full, size="
              + dispatchMessageQueue.size() + ", capacity=" + dispatchQueueCapacity);
        }
      }
    } catch (Throwable x) {
      metricsService.incrementAtomicInteger(FailedMsg.PERIOD_CHECK);
      LOG.error("put PERIOD_CHECK event to queue failed ", x);
    }
  }


  private void addToGetQueueForServer(ParameterServerId serverId, Request request) {
    request.rpcTimes.clientTimes = new ClientTimes();
    request.rpcTimes.clientTimes.requestStart = System.currentTimeMillis();

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
    request.rpcTimes.clientTimes = new ClientTimes();
    request.rpcTimes.clientTimes.requestStart = System.currentTimeMillis();

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
    GetUDFRequest requestData = new GetUDFRequest(func.getClass().getName(), param, func.getParam());

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
    header.setMatrixId(matrixId);
    header.setPartId(partId);
    return header;
  }

  /**
   * One iteration of the PS-location-refresher: for every PS marked as failed, ask the master
   * for its (possibly new) location and, on update, route a REFRESH_SERVER_LOCATION_SUCCESS
   * event so the dispatcher reschedules pending requests. Replaces the original infinite
   * Thread loop; the scheduler invokes this with the configured period.
   */
  private void runPSLocRefreshOnce() {
    try {
      if (System.currentTimeMillis() - lastMasterHbTs > hbTimeoutMS) {
        metricsService.incrementAtomicInteger(FailedMsg.HEARTBEAT_TIMEOUT);
        LOG.fatal("can not connect to master in " + hbTimeoutMS + " ms!!");
        PSAgentContext.get().getUserRequestAdapter().clear("Angel master is exit!!");
      }

      if (failedPSToLocMap.isEmpty()) {
        lastMasterHbTs = System.currentTimeMillis();
        return;
      }

      Iterator<Entry<ParameterServerId, Location>> iter = failedPSToLocMap.entrySet().iterator();
      while (iter.hasNext()) {
        final Entry<ParameterServerId, Location> entry = iter.next();
        try {
          // Time-bound the master RPC: a stuck master Netty channel can otherwise block this
          // single PSLocRefresher thread for the OS TCP retransmit window (tens of seconds
          // to minutes), during which NO further failed PS can be refreshed. By dispatching
          // to masterRpcExecutor + Future.get(timeoutMs) we cap each call at a few seconds
          // and let the next 5s tick retry. The thread that runs the RPC is from the
          // isolated masterRpcExecutor pool, so a stuck call only consumes one slot there.
          Location loc = callMasterWithTimeout(
              "getPSLocation(" + entry.getKey() + ")",
              new java.util.concurrent.Callable<Location>() {
                @Override public Location call() throws Exception {
                  return PSAgentContext.get().getMasterClient().getPSLocation(entry.getKey());
                }
              });
          lastMasterHbTs = System.currentTimeMillis();
          if (loc != null && !loc.equals(entry.getValue())) {
            LOG.info("Refresh location for PS " + entry.getKey() + ", new location=" + loc);
            PSAgentContext.get().getLocationManager().setPsLocation(entry.getKey(), loc);
            removeFailedServers(entry.getKey());
            refreshServerLocationSuccess(entry.getKey(), true);
          }
        } catch (Throwable e) {
          metricsService.incrementAtomicInteger(FailedMsg.GET_LOCATION_FAILED);
          LOG.error("Get location from master failed ", e);
        }
      }
    } catch (Throwable outer) {
      // Never let an exception escape the scheduled task -- ScheduledExecutorService would
      // otherwise silently stop re-scheduling it.
      LOG.error("PSLocRefresher iteration failed", outer);
    }
  }

  /**
   * Run {@code task} on {@link #masterRpcExecutor} and wait at most {@link #masterRpcTimeoutMs}
   * milliseconds for its result. Cancels the underlying future on timeout so the worker
   * thread is freed (best-effort; protobuf RPC isn't always interruptible, but cancelling
   * at least stops any subsequent {@link java.util.concurrent.Future#get} from waiting).
   *
   * <p>Why submit + get(timeout) rather than calling synchronously: the protobuf channel
   * underlying {@code MasterClient} does not honour any client-side deadline by default,
   * so a synchronous call can block for the OS TCP retransmit window if the master Netty
   * channel is stuck. Time-bounding at this layer is the cheapest fix that doesn't require
   * touching the RPC stack.
   *
   * <p>Package-private + parameterised purely for the call-sites in this class; the helper
   * intentionally does no logging of its own — callers already log richer context.
   *
   * @throws java.util.concurrent.TimeoutException if the call exceeds {@code masterRpcTimeoutMs}
   * @throws Exception any exception thrown by {@code task} (unwrapped from ExecutionException)
   */
  private <T> T callMasterWithTimeout(String desc, java.util.concurrent.Callable<T> task)
      throws Exception {
    java.util.concurrent.ExecutorService exec = masterRpcExecutor;
    if (exec == null || exec.isShutdown()) {
      // Adapter is winding down; fall back to synchronous to avoid losing the call entirely.
      return task.call();
    }
    java.util.concurrent.Future<T> f = exec.submit(task);
    try {
      return f.get(masterRpcTimeoutMs, java.util.concurrent.TimeUnit.MILLISECONDS);
    } catch (java.util.concurrent.TimeoutException te) {
      // Cancel best-effort. We rethrow so the caller logs and metric-bumps consistently
      // with other failure modes; the "desc" parameter goes into the exception message so
      // operator log review can tell which master RPC timed out.
      f.cancel(true);
      throw new java.util.concurrent.TimeoutException(
          "master RPC " + desc + " timed out after " + masterRpcTimeoutMs + " ms");
    } catch (java.util.concurrent.ExecutionException ee) {
      Throwable cause = ee.getCause() != null ? ee.getCause() : ee;
      if (cause instanceof Exception) throw (Exception) cause;
      if (cause instanceof Error) throw (Error) cause;
      throw new RuntimeException(cause);
    }
  }

  private boolean isRefreshing(ParameterServerId psId) {
    return failedPSToLocMap.containsKey(psId);
  }

  private void removeFailedServers(ParameterServerId psId) {
    failedPSToLocMap.remove(psId);
  }

  /**
   * Remove stale per-PS-location local state keyed by old {@link PSLocation} entries when a PS
   * refreshes to a new location. The two maps below are keyed by (psId, loc), so a location
   * change leaves old keys orphaned unless we explicitly sweep by psId.
   */
  private void cleanupPsLocationStateByPsId(ParameterServerId psId) {
    if (psId == null) {
      return;
    }

    int removedCtx = 0;
    Iterator<Entry<PSLocation, GetChannelContext>> ctxIter =
        psLocToGetChannelContextMap.entrySet().iterator();
    while (ctxIter.hasNext()) {
      Entry<PSLocation, GetChannelContext> e = ctxIter.next();
      PSLocation k = e.getKey();
      if (k != null && psId.equals(k.psId)) {
        ctxIter.remove();
        removedCtx++;
      }
    }

    int removedInactiveTs = 0;
    Iterator<Entry<PSLocation, Long>> inactiveTsIter =
        psLocToLastInactiveLogTsMap.entrySet().iterator();
    while (inactiveTsIter.hasNext()) {
      Entry<PSLocation, Long> e = inactiveTsIter.next();
      PSLocation k = e.getKey();
      if (k != null && psId.equals(k.psId)) {
        inactiveTsIter.remove();
        removedInactiveTs++;
      }
    }

    if ((removedCtx > 0 || removedInactiveTs > 0) && LOG.isInfoEnabled()) {
      LOG.info("[PS-LOC-STATE][CLEANUP] psId=" + psId
          + ", removedChannelContexts=" + removedCtx
          + ", removedInactiveLogTs=" + removedInactiveTs
          + ", remainingChannelContexts=" + psLocToGetChannelContextMap.size()
          + ", remainingInactiveLogTs=" + psLocToLastInactiveLogTsMap.size());
    }
  }

  /**
   * One iteration of the PS-heartbeat task: for every PS currently in the gray list, decide

   * whether to (a) mark it failed because PS reported EXITED, (b) mark it failed because
   * heartbeats have timed out, (c) drop it back to normal because it has recovered, or
   * (d) keep probing it with another heartbeat. Replaces the original infinite Thread loop;
   * the scheduler invokes this with the configured interval.
   */
  private void runPSHeartbeatOnce() {
    try {
      int psHbTimeOutMS = PSAgentContext.get().getConf().getInt(
          AngelConf.ANGEL_PSAGENT_TO_PS_HEARTBEAT_TIMEOUT_MS,
          AngelConf.DEFAULT_ANGEL_PSAGENT_TO_PS_HEARTBEAT_TIMEOUT_MS);

      if (LOG.isInfoEnabled()) {
        LOG.info("[GRAY-LIST][HEARTBEAT-ROUND] size=" + grayServers.size()
            + ", timeoutMs=" + psHbTimeOutMS);
      }
      Iterator<Entry<ParameterServerId, GrayServer>> iter = grayServers.entrySet().iterator();
      while (iter.hasNext()) {
        Entry<ParameterServerId, GrayServer> entry = iter.next();
        GrayServer gs = entry.getValue();
        LOG.warn("[GRAY-LIST][PROBE] psId=" + entry.getKey() + ", psLoc=" + gs.psLoc
            + ", state=" + gs.state + ", lastHbTs=" + gs.lastHBTs);


        if (gs.state == ServerState.EXITED) {
          metricsService.incrementAtomicInteger(FailedMsg.CHECK_PS_EXIST_OR_NOT);
          LOG.error("PS " + gs.psLoc + " already exited, just remove from gray"
              + " list and put it to failed list");
          try {
            notifyServerFailed(gs, false);
          } catch (ServiceException e) {
            continue;
          }
          iter.remove();
          if (LOG.isInfoEnabled()) {
            LOG.info("[GRAY-LIST][REMOVE-IN-HEARTBEAT] reason=ps-exited, psId=" + entry.getKey()
                + ", psLoc=" + gs.psLoc + ", remaining=" + grayServers.size());
          }
          continue;

        }

        if (System.currentTimeMillis() - gs.lastHBTs > psHbTimeOutMS) {
          metricsService.incrementAtomicInteger(FailedMsg.CHECK_HEARTBEAT_TIMEOUT);
          LOG.error("PS " + gs.psLoc + " heartbeat timeout, notify to Master");
          try {
            notifyServerFailed(gs, true);
          } catch (ServiceException e) {
            LOG.error("Notify PS " + gs.psLoc + " failed message to Master failed ", e);
            continue;
          }
          iter.remove();
          if (LOG.isInfoEnabled()) {
            LOG.info("[GRAY-LIST][REMOVE-IN-HEARTBEAT] reason=heartbeat-timeout, psId=" + entry.getKey()
                + ", psLoc=" + gs.psLoc + ", remaining=" + grayServers.size());
          }
          continue;

        }

        if (gs.state != null
            && (gs.state == ServerState.IDLE || gs.state == ServerState.GENERAL)) {
          LOG.info("PS " + gs.psLoc + " state back to normal, remove it from gray server list");
          iter.remove();
          if (LOG.isInfoEnabled()) {
            LOG.info("[GRAY-LIST][RECOVERED] psId=" + entry.getKey()
                + ", psLoc=" + gs.psLoc + ", state=" + gs.state
                + ", remaining=" + grayServers.size());
          }
          notifyServerNormal(gs);
          continue;
        }


        sendHeartBeat(entry.getKey(), gs.psLoc);
      }
    } catch (Throwable outer) {
      LOG.error("PSHeartbeat iteration failed", outer);
    }
  }

  private void notifyServerFailed(GrayServer server, boolean notifyToMaster)
      throws ServiceException {
    try {
      dispatchMessageQueue.put(new ServerEvent(EventType.SERVER_FAILED, server.psLoc));
    } catch (Exception e) {
      metricsService.incrementAtomicInteger(FailedMsg.SERVER_FAILED);
      LOG.error("add SERVER_FAILED event for request failed, ", e);
    }

    if (notifyToMaster) {
      // Time-bound the report so we don't pin the heartbeater iteration on a stuck master.
      // ServiceException is the historical declared throwable, so we unwrap anything else
      // into a ServiceException to preserve the existing contract.
      final PSLocation loc = server.psLoc;
      try {
        callMasterWithTimeout("psFailed(" + loc + ")",
            new java.util.concurrent.Callable<Void>() {
              @Override public Void call() throws Exception {
                PSAgentContext.get().getMasterClient().psFailed(loc);
                return null;
              }
            });
      } catch (ServiceException se) {
        throw se;
      } catch (Exception e) {
        throw new ServiceException(e);
      }
    }
  }

  private void notifyServerNormal(GrayServer server) {
    psIdToStateMap.put(server.psLoc.psId, server.state);
    try {
      dispatchMessageQueue.put(new ServerEvent(EventType.SERVER_NORMAL, server.psLoc));
    } catch (Exception e) {
      metricsService.incrementAtomicInteger(FailedMsg.ADD_SERVER_NORMAL_FAILED);
      LOG.error("add SERVER_NORMAL event for request failed, ", e);
    }
  }

  private void sendHeartBeat(ParameterServerId psId, PSLocation psLoc) {
    hbThreadPool.execute(() -> {
      try {
        LOG.info("Start to send PS heartbeat to " + psLoc);
        // Time-bound the PS-state probe so a stuck PS Netty channel can't pin this
        // hbThreadPool worker for the OS TCP retransmit window. If every probe thread
        // gets pinned, the heartbeater can't observe any PS recovering. Routed through
        // masterRpcExecutor (despite the name) because the timeout-wrapper semantics
        // are identical and we want a single isolated pool for ALL time-bounded
        // control-plane RPCs.
        ServerState state = callMasterWithTimeout(
            "getState(" + psLoc + ")",
            () -> PSAgentContext.get().getPSControlClientManager()
                .getOrCreatePSClient(psLoc.loc).getState());

        int dataProbeTimeoutMs = getHeartbeatDataProbeTimeoutMs();
        Location dataLoc = probeDataPort(psLoc, dataProbeTimeoutMs);

        LOG.info("PS heartbeat to " + psLoc + " succeeded, state=" + state
            + ", controlPortOk=true, dataPortOk=true, dataLoc=" + dataLoc
            + ", dataProbeTimeoutMs=" + dataProbeTimeoutMs);
        GrayServer server = grayServers.get(psId);
        if (server != null) {
          server.state = state;
          server.lastHBTs = System.currentTimeMillis();
          LOG.info("Updated gray server " + psId + " state to " + state + ", lastHBTs=" + server.lastHBTs);
        }
      } catch (Throwable e) {
        metricsService.incrementAtomicInteger(FailedMsg.SEND_HEARTBEAT_FAILED);
        LOG.error("Send heartbeat to " + psLoc + " failed (control/data port check)", e);

        // Check PS restart or not. Silently swallowing exceptions here used to make
        // "why didn't the gray PS ever transition to EXITED?" impossible to debug;
        // log at DEBUG so it shows up under -X verbose runs without spamming normal logs.
        // Same time-bound applies — we don't want the failure-path probe to hang either.
        try {
          boolean exited = callMasterWithTimeout(
              "isPSExited(" + psLoc + ")",
              () -> PSAgentContext.get().getMasterClient().isPSExited(psLoc));
          if (exited) {
            GrayServer server = grayServers.get(psLoc.psId);
            if (server != null) {
              server.state = ServerState.EXITED;
              server.lastHBTs = System.currentTimeMillis();
            }
          }
        } catch (Throwable x) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("isPSExited(" + psLoc + ") probe failed; will retry next heartbeat", x);
          }
        }
      }
    });
  }

  private int getHeartbeatDataProbeTimeoutMs() {
    int configured = PSAgentContext.get().getConf().getInt(
        AngelConf.ANGEL_NETTY_MATRIXTRANSFER_CLIENT_GET_CONN_TIMEOUT_MS,
        AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_CLIENT_GET_CONN_TIMEOUT_MS);
    // Heartbeat check should be quick: too large timeout blocks the heartbeat worker and
    // delays gray-list recovery/failure judgement. Clamp to [500ms, 3000ms].
    return Math.max(500, Math.min(configured, 3000));
  }

  private Location probeDataPort(PSLocation psLoc, int timeoutMs) throws IOException {
    if (psLoc == null || psLoc.loc == null) {
      throw new IOException("data port probe failed: psLoc is null");
    }

    Location dataLoc = new Location(psLoc.loc.getIp(), psLoc.loc.getPort() + 1);
    ChannelFuture connectFuture = bootstrap.connect(dataLoc.getIp(), dataLoc.getPort());
    boolean done = connectFuture.awaitUninterruptibly(timeoutMs);
    if (!done) {
      connectFuture.cancel(true);
      throw new IOException("data port probe timeout after " + timeoutMs + " ms, dataLoc=" + dataLoc);
    }

    if (!connectFuture.isSuccess()) {
      Throwable cause = connectFuture.cause();
      throw new IOException("data port probe connect failed, dataLoc=" + dataLoc,
          cause == null ? new RuntimeException("unknown") : cause);
    }

    Channel channel = connectFuture.channel();
    boolean active = channel != null && channel.isActive();
    boolean open = channel != null && channel.isOpen();
    if (!active || !open) {
      if (channel != null) {
        channel.close();
      }
      throw new IOException("data port probe got inactive channel, dataLoc=" + dataLoc
          + ", active=" + active + ", open=" + open);
    }

    if (channel != null) {
      channel.close();
    }

    return dataLoc;
  }


  // genSeqId() removed: never called. The actual seqId source is the inline
  // currentSeqId.incrementAndGet() in RequestDispatcher.submit().

  private boolean isFailed(ParameterServerId psId) {
    return failedPSToLocMap.containsKey(psId);
  }

  private boolean isInGrayList(ParameterServerId psId) {
    return grayServers.containsKey(psId);
  }

  private void removeFromGrayList(ParameterServerId psId) {
    removeFromGrayList(psId, "unspecified");
  }

  private void removeFromGrayList(ParameterServerId psId, String reason) {
    GrayServer removed = grayServers.remove(psId);
    if (LOG.isInfoEnabled()) {
      LOG.info("[GRAY-LIST][REMOVE] reason=" + reason
          + ", psId=" + psId
          + ", removed=" + (removed != null)
          + ", remaining=" + grayServers.size()
          + (removed == null ? "" : ", psLoc=" + removed.psLoc + ", state=" + removed.state));
    }
  }

  private void addToGrayList(PSLocation psLoc) {
    addToGrayList(psLoc, "unspecified");
  }

  private void addToGrayList(PSLocation psLoc, String reason) {
    GrayServer server = new GrayServer(psLoc, System.currentTimeMillis());
    GrayServer old = grayServers.putIfAbsent(psLoc.psId, server);
    if (LOG.isInfoEnabled()) {
      LOG.info("[GRAY-LIST][ADD] reason=" + reason
          + ", psLoc=" + psLoc
          + ", added=" + (old == null)
          + ", size=" + grayServers.size()
          + (old == null ? "" : ", existState=" + old.state + ", lastHbTs=" + old.lastHBTs));
    }
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
     * Un-schedulable failed partition request queues: requests sit here while their back-off
     * window is still in the future or while their target PS is grayed / failed. They migrate
     * to the schedulable* queues as soon as both conditions clear.
     *
     * <p>Previously these were {@link LinkedList}s walked top-to-bottom every tick, which is
     * O(N) per tick. They are now min-heaps ordered by {@code RequestContext.nextRetryTs}, so
     * the per-tick promotion logic only inspects entries whose back-off has actually expired
     * (a {@code peek}-and-poll loop terminates the moment the heap top is not yet due). A small
     * {@code bypass} list captures requests whose back-off has expired but whose target PS is
     * temporarily unschedulable, so they are reinserted in O(K) (K = bypassed count) instead
     * of being polled-then-reoffered which would re-pivot the heap on the same key.</p>
     */
    private final java.util.PriorityQueue<Request> failedPutCache;
    private final java.util.PriorityQueue<Request> failedGetCache;

    private final java.util.Comparator<Request> BY_NEXT_RETRY_TS =
        new java.util.Comparator<Request>() {
          @Override public int compare(Request a, Request b) {
            return Long.compare(
                a.getContext().getNextRetryTs(),
                b.getContext().getNextRetryTs());
          }
        };

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

    /**
     * Upper bound (ms) of the random back-off window used by {@link #genNextRetryTs}. Decoupled
     * from {@link #requestTimeOut} so the latter can stay large to accommodate slow PSFs while
     * transient network errors still recover in seconds. See
     * {@link AngelConf#ANGEL_MATRIXTRANSFER_RETRY_BACKOFF_WINDOW_MS}.
     */
    private final int retryBackoffWindowMs;

    private final int psHbTimeOutMS;

    private int tickClock;

    /**
     * Number of consecutive unexpected throwables tolerated before treating the dispatcher as
     * fatally broken. Previously a single Throwable in any event handler would unconditionally
     * call {@code psAgent.error(...)} and kill the whole worker — an overreaction for what
     * are often transient races (e.g. NPE during shutdown). We now log + continue up to this
     * many times before escalating.
     */
    private static final int FATAL_THROWABLE_THRESHOLD = 64;
    private int consecutiveThrowables = 0;

    public RequestDispatcher() {
      Configuration conf = PSAgentContext.get().getConf();
      psIds = PSAgentContext.get().getLocationManager().getPsIds();
      lastChosenPutServerIndex = new Random().nextInt(psIds.length);
      lastChosenGetServerIndex = new Random().nextInt(psIds.length);

      failedPutCache = new java.util.PriorityQueue<>(64, BY_NEXT_RETRY_TS);
      failedGetCache = new java.util.PriorityQueue<>(64, BY_NEXT_RETRY_TS);
      schedulableFailedPutCache = new LinkedBlockingQueue<>();
      schedulableFailedGetCache = new LinkedBlockingQueue<>();

      requestTimeOut = conf.getInt(AngelConf.ANGEL_MATRIXTRANSFER_REQUEST_TIMEOUT_MS,
          AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_REQUEST_TIMEOUT_MS);

      retryBackoffWindowMs = conf.getInt(AngelConf.ANGEL_MATRIXTRANSFER_RETRY_BACKOFF_WINDOW_MS,
          AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_RETRY_BACKOFF_WINDOW_MS);

      psHbTimeOutMS = conf.getInt(AngelConf.ANGEL_PSAGENT_TO_PS_HEARTBEAT_TIMEOUT_MS,
          AngelConf.DEFAULT_ANGEL_PSAGENT_TO_PS_HEARTBEAT_TIMEOUT_MS);

      tickClock = 0;
      LOG.info("ByteOrder.nativeOrder()=" + ByteOrder.nativeOrder());
    }

    private static final int EVENT_BATCH_MAX = 256;
    private final java.util.ArrayList<DispatcherEvent> eventBatch =
        new java.util.ArrayList<>(EVENT_BATCH_MAX);

    @Override
    public void run() {
      while (!stopped.get() && !Thread.interrupted()) {
        try {
          // Block for the first event, then opportunistically batch-drain whatever else is
          // already queued. The original "take one, process one, repeat" loop made the
          // dispatcher pay the lock/wakeup cost of LinkedBlockingQueue per event, and ran
          // getDataSplit/putDataSplit once per success event even though N back-to-back
          // SUCCESS events have exactly the same effect as one. With batching we collapse
          // those into a single scheduler invocation at the end of the batch.
          eventBatch.clear();
          eventBatch.add(dispatchMessageQueue.take());
          dispatchMessageQueue.drainTo(eventBatch, EVENT_BATCH_MAX - 1);

          boolean needGetSplit = false;
          boolean needPutSplit = false;
          boolean needPeriodCheck = false;
          int periodCheckCount = 0;

          for (int i = 0; i < eventBatch.size(); i++) {
            DispatcherEvent event = eventBatch.get(i);
            if (LOG.isDebugEnabled()) {
              LOG.debug("handler event " + event);
            }
            switch (event.getType()) {
              case START_GET:
              case GET_SUCCESS:
                needGetSplit = true;
                break;

              case START_PUT:
              case PUT_SUCCESS:
                needPutSplit = true;
                break;

              case GET_FAILED: {
                RequestFailedEvent getFailedEvent = (RequestFailedEvent) event;
                Request request = getFailedEvent.getRequest();
                long nowTs = System.currentTimeMillis();
                long nextRetryTs = genNextRetryTs(
                    getFailedEvent.getFailedType(), request.getContext().getTryCounter());
                request.getContext().setFailedTs(nowTs);
                request.getContext().setNextRetryTs(nextRetryTs);
                failedGetCache.add(request);
                if (LOG.isInfoEnabled()) {
                  int doneTry = request.getContext().getTryCounter();
                  String reqKey = "u" + request.getUserRequestId() + "#r" + request.hashCode();
                  LOG.info("[RPC-RETRY][SCHEDULE][GET] reqKey=" + reqKey
                      + ", userRequestId=" + request.getUserRequestId()
                      + ", failedSeqId=" + request.getSeqId()
                      + ", doneTry=" + doneTry
                      + ", nextTry=" + (doneTry + 1)
                      + ", retryAfterMs=" + Math.max(0L, nextRetryTs - nowTs)
                      + ", failedType=" + getFailedEvent.getFailedType()
                      + ", serverId=" + request.getContext().getActualServerId()
                      + ", loc=" + request.getContext().getLocation()
                      + ", failedLog=" + getFailedEvent.getFailedLog());
                }
                if (needPutToGrayServers(getFailedEvent.getFailedType())) {
                  addToGrayList(new PSLocation(request.getContext().getActualServerId(),
                      request.getContext().getLocation()), "request-get-failed-" + getFailedEvent.getFailedType());
                }

                needGetSplit = true;
                break;
              }

              case PUT_FAILED: {
                RequestFailedEvent putFailedEvent = (RequestFailedEvent) event;
                Request request = putFailedEvent.getRequest();
                long nowTs = System.currentTimeMillis();
                long nextRetryTs = genNextRetryTs(
                    putFailedEvent.getFailedType(), request.getContext().getTryCounter());
                request.getContext().setFailedTs(nowTs);
                request.getContext().setNextRetryTs(nextRetryTs);
                failedPutCache.add(request);
                if (LOG.isInfoEnabled()) {
                  int doneTry = request.getContext().getTryCounter();
                  String reqKey = "u" + request.getUserRequestId() + "#r" + request.hashCode();
                  LOG.info("[RPC-RETRY][SCHEDULE][PUT] reqKey=" + reqKey
                      + ", userRequestId=" + request.getUserRequestId()
                      + ", failedSeqId=" + request.getSeqId()
                      + ", doneTry=" + doneTry
                      + ", nextTry=" + (doneTry + 1)
                      + ", retryAfterMs=" + Math.max(0L, nextRetryTs - nowTs)
                      + ", failedType=" + putFailedEvent.getFailedType()
                      + ", serverId=" + request.getContext().getActualServerId()
                      + ", loc=" + request.getContext().getLocation()
                      + ", failedLog=" + putFailedEvent.getFailedLog());
                }
                if (needPutToGrayServers(putFailedEvent.getFailedType())) {
                  addToGrayList(new PSLocation(request.getContext().getActualServerId(),
                      request.getContext().getLocation()), "request-put-failed-" + putFailedEvent.getFailedType());
                }

                needPutSplit = true;
                break;
              }

              case SERVER_FAILED: {
                ServerEvent serverFailedEvent = (ServerEvent) event;
                failedPSToLocMap.put(
                    serverFailedEvent.getPsLoc().psId, serverFailedEvent.getPsLoc().loc);
                removeFromGrayList(serverFailedEvent.getPsLoc().psId, "server-failed-event");

                psFailed(serverFailedEvent.getPsLoc());
                break;
              }

              case SERVER_NORMAL: {
                ServerEvent serverNormalEvent = (ServerEvent) event;
                removeFromGrayList(serverNormalEvent.getPsLoc().psId, "server-normal-event");

                psNormal(serverNormalEvent.getPsLoc());
                break;
              }

              case REFRESH_SERVER_LOCATION_SUCCESS: {
                RefreshServerLocationEvent refreshEvent = (RefreshServerLocationEvent) event;
                ParameterServerId serverId = refreshEvent.getServerId();
                LOG.info("refresh location for server " + serverId + " success ");
                failedPSToLocMap.remove(serverId);
                cleanupPsLocationStateByPsId(serverId);
                // Targeted dispatch for the recovered server; cannot be coalesced with the
                // batch-end "needGetSplit/needPutSplit" because those scan ALL PSs.
                dispatchTransportEvent(serverId);
                break;
              }


              case PERIOD_CHECK: {
                // Coalesce: N PERIOD_CHECK events in a single batch behave exactly like one.
                // tickClock still advances for every event so checkChannelContext / debug-print
                // gating keeps the same cadence as before.
                tickClock++;
                periodCheckCount++;
                needPeriodCheck = true;
                break;
              }

              case CHANNEL_CLOSED: {
                ChannelClosedEvent closeEvent = (ChannelClosedEvent) event;
                removeRequestForChannel(closeEvent.getChannel(), closeEvent.getReason());
                // Want a full rescan because the dead channel may have carried inflight RPCs
                // to multiple PSs (router cache).
                needGetSplit = true;
                needPutSplit = true;
                break;
              }


              default:
                break;
            }
          }

          if (needPeriodCheck) {
            if (periodCheckCount > 1 && LOG.isDebugEnabled()) {
              LOG.debug("collapsed " + periodCheckCount + " PERIOD_CHECK events into one");
            }
            dispatchTransportEvent(null);
          } else {
            if (needPutSplit) putDataSplit();
            if (needGetSplit) getDataSplit();
          }
          // Successful loop iteration -- reset the consecutive-failure counter.
          consecutiveThrowables = 0;
        } catch (InterruptedException e) {
          if (!stopped.get()) {
            metricsService.incrementAtomicInteger(FailedMsg.REQUEST_DISPATCHER_IS_INTERRUPTED);
            LOG.fatal("RequestDispatcher is interrupted!! ", e);
            PSAgentContext.get().getPsAgent().error("RequestDispatcher is interrupted!!");
          }
          // Interrupted -> exit the loop; the stop() path is the only legitimate caller.
          return;
        } catch (Throwable e) {
          metricsService.incrementAtomicInteger(FailedMsg.REQUEST_DISPATCHER_IS_FAILED);
          consecutiveThrowables++;
          if (consecutiveThrowables >= FATAL_THROWABLE_THRESHOLD) {
            LOG.fatal("RequestDispatcher hit " + consecutiveThrowables
                + " consecutive throwables, giving up", e);
            PSAgentContext.get().getPsAgent().error(
                "RequestDispatcher is failed after " + consecutiveThrowables
                    + " consecutive errors. " + e.getMessage());
            return;
          } else {
            LOG.error("RequestDispatcher event handling failed (#"
                + consecutiveThrowables + "), continue", e);
          }
        }
      }
    }

    /**
     * Delegates to {@link RetryPolicy#nextRetryTs}. The dispatcher used to inline the
     * back-off computation here, but that made it impossible to test without standing up a
     * full PSAgent. The policy is now its own class and this method is a thin shim.
     */
    private long genNextRetryTs(ResponseType failedType, int tryCounter) {
      return retryPolicy.nextRetryTs(System.currentTimeMillis(), failedType, tryCounter);
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
      item.rpcTimes.clientTimes.requestScheduleStart = System.currentTimeMillis();

      int seqId = currentSeqId.incrementAndGet();
      item.setSeqId(seqId);
      item.getContext().setWaitTimeTicks(0);
      rpcContext.before(item.getContext().getServerId());
      LOG.debug("submit request seqId=" + seqId + ",request=" + item);
      if (item.getContext().getTryCounter() > 0 && LOG.isInfoEnabled()) {
        int doneTry = item.getContext().getTryCounter();
        String reqKey = "u" + item.getUserRequestId() + "#r" + item.hashCode();
        LOG.info("[RPC-RETRY][SUBMIT] reqKey=" + reqKey
            + ", userRequestId=" + item.getUserRequestId()
            + ", submitSeqId=" + seqId
            + ", doneTry=" + doneTry
            + ", thisTry=" + (doneTry + 1)
            + ", serverId=" + item.getContext().getServerId()
            + ", lastServerId=" + item.getContext().getActualServerId()
            + ", lastLoc=" + item.getContext().getLocation()
            + ", nextRetryTs=" + item.getContext().getNextRetryTs());
      }
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
      // Two callers exist:
      //   1) PERIOD_CHECK ticks the clock and calls dispatchTransportEvent(null) every event
      //      loop iteration; this is the steady-state "drain back-off-expired requests" path.
      //      Throttling it to 1-in-10 ticks (~1s with checkPeriodMS=100ms) is the right
      //      thing — the failed-request heap doesn't change fast enough to need 100Hz scans.
      //   2) REFRESH_SERVER_LOCATION_SUCCESS fires when the master has just told us the new
      //      address of a PS that we previously gave up on. This carries a non-null
      //      serverId and means "I have a freshly-recovered PS, please dispatch its parked
      //      requests RIGHT NOW". The historical 1-in-10 gate silently dropped 9 out of 10
      //      such events, leaving the client visually stuck for an extra 0..1s every time
      //      a PS came back — and *much* longer if a recovery event collided with a tick
      //      boundary that never produced a follow-up tickClock%10==0 (e.g. when the
      //      dispatcher loop is doing nothing else and tickClock isn't advancing). That is
      //      the "PS attempt_1 successfully registered, but client still hangs" symptom we
      //      observed in production. Bypass the throttle whenever serverId != null.
      boolean isTargetedRecovery = (serverId != null);
      if (!isTargetedRecovery && tickClock % 10 != 0) {
        return;
      }

      // Check all pending RPCS
      checkTimeout();

      // Check get channel context. Only on the periodic path; a targeted recovery does not
      // need to redo the per-PS health window scan and we don't want it to reset counters.
      if (!isTargetedRecovery && tickClock % 10 == 0) {
        checkChannelContext();
      }

      // Promote all back-off-expired requests to the schedulable queues. Uses the heap order
      // by nextRetryTs: we peek the head, stop the moment its retry time is in the future,
      // and only spend O(K log N) instead of O(N) per tick (K = number actually promoted).
      // Bypassed entries (expired back-off but PS is currently gray/failed/foreign) are held
      // in a small list and reinserted in one shot at the end so we don't keep re-pivoting
      // the heap on the same key.
      //
      // For targeted recovery we additionally force-promote any request belonging to the
      // recovered PS regardless of its remaining back-off: the back-off was sized for "PS
      // is sick", but we just got first-hand evidence from the master that the PS is back
      // at a new location, so honoring the back-off would only delay the very recovery the
      // event was raised for.
      long ts = System.currentTimeMillis();
      if (isTargetedRecovery) {
        forcePromoteForRecoveredPS(failedPutCache, schedulableFailedPutCache, serverId);
        forcePromoteForRecoveredPS(failedGetCache, schedulableFailedGetCache, serverId);
      }
      drainExpiredFailedRequests(failedPutCache, schedulableFailedPutCache, serverId, ts);
      putDataSplit();

      drainExpiredFailedRequests(failedGetCache, schedulableFailedGetCache, serverId, ts);
      getDataSplit();
      if (!isTargetedRecovery && (tickClock % 100 == 0) && LOG.isDebugEnabled()) {
        printDispatchInfo();
        //channelManager.printPools();
      }
    }

    /**
     * Move every request belonging to {@code recoveredServerId} from {@code from} to
     * {@code to} unconditionally — bypassing the back-off check. Caller is the
     * REFRESH_SERVER_LOCATION_SUCCESS event handler which has just learned the PS is back
     * at a new address; making the parked requests wait for their old back-off would
     * defeat the purpose of the recovery event.
     */
    private void forcePromoteForRecoveredPS(java.util.PriorityQueue<Request> from,
        LinkedBlockingQueue<Request> to, ParameterServerId recoveredServerId) {
      int moved = forcePromoteForRecoveredPSImpl(from, to, recoveredServerId);
      if (moved > 0 && LOG.isInfoEnabled()) {
        LOG.info("PS " + recoveredServerId + " recovered: force-promoted "
            + moved + " parked request(s) bypassing their remaining back-off");
      }
    }

    /**
     * Move every expired-back-off request from {@code from} to {@code to}, subject to the
     * usual gating: target PS not in gray list, not failed, and either we are scanning all
     * PS ({@code serverId == null}) or this request belongs to the targeted PS.
     *
     * <p>Algorithm: heap-pop in nextRetryTs order, stop the moment the top's back-off is in
     * the future; entries that passed the back-off check but fail the PS-state gating are
     * stashed in {@code bypass} and bulk-reinserted at the end. This keeps the per-tick cost
     * proportional to "really-promoted + really-bypassed" rather than the total heap size.</p>
     */
    private void drainExpiredFailedRequests(java.util.PriorityQueue<Request> from,
        LinkedBlockingQueue<Request> to, ParameterServerId serverId, long nowTs) {
      java.util.ArrayList<Request> bypass = null;
      while (true) {
        Request top = from.peek();
        if (top == null) {
          break;
        }
        if (nowTs - top.getContext().getNextRetryTs() < 0) {
          // Heap top is the earliest one; if it's not yet due, none are.
          break;
        }
        Request item = from.poll();
        ParameterServerId itemSrv = item.getContext().getServerId();
        boolean schedulable =
            (serverId == null || itemSrv == serverId)
                && !isInGrayList(itemSrv)
                && !isFailed(itemSrv);
        if (schedulable) {
          to.add(item);
        } else {
          if (bypass == null) {
            bypass = new java.util.ArrayList<>(4);
          }
          bypass.add(item);
        }
      }
      if (bypass != null) {
        from.addAll(bypass);
      }
    }

    private void checkChannelContext() {
      long startTs = System.currentTimeMillis();

      // Fault injection: force a synthetic "inactive" observation on the listed PSes
      // before we evaluate the per-window predicate, so the consecutive-window detector
      // trips on the configured ids without needing real network failures. See AngelConf.
      // Snapshot the set into a local to avoid racing with a concurrent config reload.
      java.util.Set<Integer> inject = faultInjectStuckPsIds;
      if (!inject.isEmpty()) {
        for (Entry<PSLocation, GetChannelContext> e : psLocToGetChannelContextMap.entrySet()) {
          if (e.getKey().psId != null && inject.contains(e.getKey().psId.getIndex())) {
            e.getValue().channelNotactive();
          }
        }
      }

      for (Entry<PSLocation, GetChannelContext> entry : psLocToGetChannelContextMap.entrySet()) {
        PSLocation psLoc = entry.getKey();
        GetChannelContext ctx = entry.getValue();
        // Snapshot the three counters once: a single sum() per LongAdder is much cheaper
        // than repeating it across the predicate and the log line, and it also keeps the
        // predicate / log self-consistent in the face of concurrent increments.
        int inactiveCnt = ctx.getInactiveCounter();
        int successCnt = ctx.getSuccessCounter();
        int failedCnt = ctx.getFailedCounter();

        boolean noProgressTimeout =
            (startTs - ctx.getLastCheckTs() > requestTimeOut * 2)
                && successCnt == 0 && failedCnt > 0;

        if (inactiveCnt > 0 || noProgressTimeout) {
          metricsService.incrementAtomicInteger(FailedMsg.NO_ACTIVE_CHANNEL);

          // Cross-window stuck-PS detection. A single window of "inactive>0, success=0,
          // failed=0" is exactly the symptom of "client just dropped the dead pool, will
          // rebuild on the next request" — completely benign and resolved within one tick.
          // Several consecutive windows like this against the SAME PS, however, mean the
          // rebuilt connections are being torn down again before any RPC completes, which
          // is the canonical signature of a remote PS process restarting / being
          // OOM-killed / actively RST-ing on accept. In that case we want a load-bearing
          // ERROR (paged on by ops) rather than a stream of identical lines that all look
          // recoverable.
          int consecutive;
          if (successCnt == 0) {
            consecutive = ctx.incConsecutiveInactiveWindows();
          } else {
            ctx.clearConsecutiveInactiveWindows();
            consecutive = 0;
          }

          // Log level decision tree:
          //   - failedCnt > 0  : real RPC failures observed → ERROR (always)
          //   - consecutive>=3 : the PS made zero progress over multiple windows → ERROR
          //                      and bump the STUCK_PS metric so it surfaces on dashboards
          //   - noProgressTimeout : we waited >2x request timeout and still saw no success
          //                      with non-zero failures (rare given we already covered
          //                      failedCnt>0 above) → WARN
          //   - otherwise      : transient pool churn auto-handled by the dead-channel
          //                      replacement path → INFO (was ERROR; that was the noise
          //                      source operators kept asking about)
          String header = "PS " + psLoc + " channel-health window:";
          String body = " inactive=" + inactiveCnt + ", success=" + successCnt
              + ", failed=" + failedCnt + ", consecutiveInactiveWindows=" + consecutive;
          if (failedCnt > 0) {
            LOG.error(header + body + " (real RPC failures observed)");
          } else if (consecutive >= 3) {
            metricsService.incrementAtomicInteger(FailedMsg.STUCK_PS);
            LOG.error(header + body
                + " (no forward progress for " + consecutive + " consecutive windows;"
                + " PS likely restarting / being OOM-killed / RST-ing on accept)");

            // Fail-safe: actively trigger the location-refresh path so the PSLocRefresher
            // queries master for the new address. Without this, recovery depends on the
            // gray-list -> heartbeat-timeout -> SERVER_FAILED chain firing, which can be
            // missed entirely if the PS dies fast enough that the client never observes a
            // proper "heartbeat timeout" before reconnecting onto the freshly-killed PID
            // and seeing only RST. We've reproduced exactly that pattern: the AM logs
            // "PSAttempt_X_1 register finished" within seconds, but the client keeps
            // hammering the OLD address because failedPSToLocMap was never populated. The
            // putIfAbsent guard keeps this idempotent if the regular path also fires.
            if (psLoc.psId != null && psLoc.loc != null
                && failedPSToLocMap.putIfAbsent(psLoc.psId, psLoc.loc) == null) {
              LOG.error("Marking PS " + psLoc + " as failed for location refresh"
                  + " (forced by stuck-PS detector)");
              // Don't wait for the next 5s PSLocRefresher tick — kick it now. We submit to
              // bgScheduler so the refresh runs on its dedicated thread (avoiding any
              // re-entrancy concerns with the dispatcher loop) and we get the new
              // location into the LocationManager within the master RPC round-trip rather
              // than within "round-trip + up-to-5s scheduler delay".
              if (bgScheduler != null && !bgScheduler.isShutdown()) {
                try {
                  bgScheduler.submit(new Runnable() {
                    @Override public void run() {
                      if (!stopped.get()) {
                        runPSLocRefreshOnce();
                      }
                    }
                  });
                } catch (Throwable t) {
                  // Best-effort; the periodic 5s tick will pick it up.
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("immediate location-refresh submit failed", t);
                  }
                }
              }
            }
          } else if (noProgressTimeout) {
            LOG.warn(header + body + " (no success within 2x request timeout)");
          } else if (LOG.isInfoEnabled()) {
            LOG.info(header + body
                + " (auto-recovered by dead-channel replacement, no RPC failed)");
          }

          // Only recycle the whole pool when the window indicates real risk of being stuck.
          // For pure transient inactive observations (failed=0, no timeout, consecutive<3),
          // one-channel eviction on borrow + pool.check() replacement is already sufficient;
          // force-closing the entire pool there just creates unnecessary churn.
          boolean shouldRecyclePool = failedCnt > 0 || noProgressTimeout || consecutive >= 3;
          if (shouldRecyclePool) {
            String recycleReason;
            if (failedCnt > 0) {
              recycleReason = "channel-health-window-has-failures";
            } else if (consecutive >= 3) {
              recycleReason = "channel-health-window-stuck-ps";
            } else {
              recycleReason = "channel-health-window-no-progress-timeout";
            }
            closeChannels(psLoc, recycleReason);
          } else if (LOG.isDebugEnabled()) {
            LOG.debug("[CHANNEL-REBUILD][POOL-CLOSE-SKIP] psLoc=" + psLoc
                + ", inactive=" + inactiveCnt
                + ", success=" + successCnt
                + ", failed=" + failedCnt
                + ", consecutiveInactiveWindows=" + consecutive);
          }
          ctx.reset();
          continue;

        }

        // Window had a real success and no inactive observation: forward progress, reset
        // the cross-window stuck counter so a later transient blip doesn't trigger the
        // STUCK_PS path on stale state.
        if (successCnt > 0) {
          ctx.clearConsecutiveInactiveWindows();
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
            metricsService.incrementAtomicInteger(FailedMsg.REQUEST_TIMEOUT);
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
        metricsService.incrementAtomicInteger(FailedMsg.REMOVE_REQUEST_FAILED);
        LOG.error("remove request failed ", x);
      }
    }

    /**
     * if a channel is closed, all request in this channel should be remove and reschedule
     *
     * @param channel closed channel
     */
    private void removeRequestForChannel(Channel channel, String closeReason) {
      // The naive "match by request.context.channel" loop used to leak: once the write
      // listener fires, returnChannel(request) sets request.context.channel = null, so
      // a channelInactive that arrives AFTER that point and BEFORE the response would
      // observe channel == null on every entry and fail nothing. Net effect: the request
      // sat in seqIdToRequestMap waiting for a response that would never come, and the
      // user-visible FutureResult.get() blocked forever (we observed Spark tasks parked
      // on it for >1h in production).
      //
      // The correct identity for "which inflight requests rode on THIS netty channel" is
      // seqIdToSendCFMap: each entry is created on the same writeAndFlush that produced
      // the channel-future, so cf.channel() is the actual carrier even after the
      // post-write returnChannel cleanup. We union both sources so a request that hasn't
      // yet hit writeAndFlush (channel set, cf not yet recorded) is still caught by the
      // legacy path.
      int removeNum = 0;
      java.util.ArrayList<String> inflightReqs = new java.util.ArrayList<>();
      String reason = closeReason == null ? "unknown" : closeReason;
      try {
        ChannelMatchResult match = matchSeqIdsForClosedChannel(
            seqIdToRequestMap, seqIdToSendCFMap, channel);
        for (Integer seqId : match.viaContextChannel) {
          removeNum++;
          inflightReqs.add(formatInFlightRequest(seqId, "context-channel"));
          requestFailed(seqId, ResponseType.NETWORK_ERROR,
              "channel is closed, reason=" + reason);
        }
        for (Integer seqId : match.viaSendCF) {
          removeNum++;
          inflightReqs.add(formatInFlightRequest(seqId, "send-cf"));
          requestFailed(seqId, ResponseType.NETWORK_ERROR,
              "channel is closed (post-write), reason=" + reason);
        }

        if (removeNum > 0) {
          LOG.info("remove channel " + channel + ", reason=" + reason + ", reschedule " + removeNum
              + " in-flight request(s), details=" + inflightReqs);
        } else if (LOG.isDebugEnabled()) {
          LOG.debug("remove channel " + channel + ", reason=" + reason
              + " (no in-flight requests attached)");
        }

        InetSocketAddress address = (InetSocketAddress) (channel.remoteAddress());
        if (address == null) {
          // Throttled: a closed channel with null remoteAddress is normal during teardown,
          // it just means we lost the original peer info. Keep at DEBUG to avoid the
          // common case where N closed channels all log this on a single shutdown.
          if (LOG.isDebugEnabled()) {
            LOG.debug("channel " + channel + " remote address is null on close");
          }
          return;
        }
        Location loc = new Location(address.getHostName(), address.getPort() - 1);
        ParameterServerId psId = PSAgentContext.get().getLocationManager().getPsId(loc);
        if (psId != null) {
          getChannelContext(new PSLocation(psId, loc)).channelNotactive();
        }

      } catch (Exception x) {
        if (!stopped.get()) {
          metricsService.incrementAtomicInteger(FailedMsg.REMOVE_REQUEST_FAILED);
          LOG.error("remove request failed ", x);
        }
      }
    }

    private String formatInFlightRequest(Integer seqId, String matchedBy) {
      Request req = seqIdToRequestMap.get(seqId);
      if (req == null) {
        return "seqId=" + seqId + ",matchedBy=" + matchedBy + ",request=already-removed";
      }
      return "seqId=" + seqId
          + ",matchedBy=" + matchedBy
          + ",userRequestId=" + req.getUserRequestId()
          + ",type=" + req.getType()
          + ",try=" + req.getContext().getTryCounter()
          + ",serverId=" + req.getContext().getServerId()
          + ",actualServerId=" + req.getContext().getActualServerId()
          + ",loc=" + req.getContext().getLocation();
    }

  }

  // --- Termination paths ------------------------------------------------------------------
  // The three terminate-an-inflight paths (success / failure-maybe-retry / fatal) all have to
  // do the same bookkeeping: rpcContext.after, drop the inflight map entry, drop the
  // seqIdToSendCFMap entry, return the channel, and (for failure paths) the serialized
  // request buffer. Each path comes in two flavours:
  //   * the legacy "by-seqId" entry, used by callers that only have the seqId (e.g. the
  //     channel-write listener, the timeout checker, the channel-closed cleanup);
  //   * the new "by-request" entry, used by Responser which has already removed the entry
  //     from seqIdToRequestMap with remove() to claim ownership atomically.
  // The by-seqId form is a thin wrapper that does the remove() then delegates. This is what
  // lets the Responser change from get() to remove() without leaking the in-flight resources
  // through "remove() then call requestSuccess(seqId) which finds null and returns".

  private void requestSuccess(int seqId) {
    Request request = seqIdToRequestMap.remove(seqId);
    if (request == null) {
      return;
    }
    requestSuccessOn(request, seqId);
  }

  private void requestSuccessOn(Request request, int seqId) {
    rpcContext.after(request.getContext().getServerId());
    seqIdToSendCFMap.remove(seqId);
    returnChannel(request);
    if (request.getContext().getTryCounter() > 1 && LOG.isInfoEnabled()) {
      String reqKey = "u" + request.getUserRequestId() + "#r" + request.hashCode();
      LOG.info("[RPC-RETRY][SUCCESS] reqKey=" + reqKey
          + ", userRequestId=" + request.getUserRequestId()
          + ", seqId=" + seqId
          + ", successTry=" + request.getContext().getTryCounter()
          + ", type=" + request.getType()
          + ", serverId=" + request.getContext().getServerId()
          + ", actualServerId=" + request.getContext().getActualServerId()
          + ", loc=" + request.getContext().getLocation());
    }
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
        metricsService.incrementAtomicInteger(FailedMsg.INVALID_RESPONSE);
        LOG.error("unvalid response for request " + request + " with seqId " + seqId);
        break;
    }
  }

  private void requestFailed(int seqId, ResponseType failedType, String errorLog) {
    Request request = seqIdToRequestMap.remove(seqId);
    if (request == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("[REQUEST-FAIL-TRACE][SKIP] seqId=" + seqId
            + ", failedType=" + failedType + ", reason=" + errorLog
            + ", request already removed");
      }
      return;
    }
    requestFailedOn(request, seqId, failedType, errorLog);
  }

  private void requestFailedOn(Request request, int seqId, ResponseType failedType,
      String errorLog) {

    // Whether we will retry or give up, the per-request resources held during the in-flight
    // window MUST be released here, otherwise:
    //   * rpcContext.inflightRPCCounter / per-server counter leak monotonically and the
    //     dispatcher's flow-control (checkIsOverReqNumLimit) eventually deadlocks all PSs;
    //   * seqIdToSendCFMap grows without bound;
    //   * the borrowed netty channel is never returned to its pool;
    //   * the serialized request ByteBuf is never released, leaking direct memory.
    // Previously these were only released on the "will-retry" path, so any FATAL response
    // (e.g. SERVER_HANDLE_FATAL, PARSE_HEADER_FAILED) or any maxTryNum-give-up silently
    // leaked all of the above. We now release first, then either notify the caller (give-up)
    // or enqueue for retry.
    rpcContext.after(request.getContext().getServerId());
    seqIdToSendCFMap.remove(seqId);
    returnChannel(request);
    returnBuffer(request);

    String reqKey = "u" + request.getUserRequestId() + "#r" + request.hashCode();
    if (LOG.isWarnEnabled()) {
      LOG.warn("[REQUEST-FAIL-TRACE][FAILED] reqKey=" + reqKey
          + ", userRequestId=" + request.getUserRequestId()
          + ", seqId=" + seqId
          + ", try=" + request.getContext().getTryCounter()
          + ", type=" + request.getType()
          + ", serverId=" + request.getContext().getServerId()
          + ", actualServerId=" + request.getContext().getActualServerId()
          + ", loc=" + request.getContext().getLocation()
          + ", failedType=" + failedType
          + ", reason=" + errorLog);
    }

    // Check need retry or not
    if (isOverTryLimit(request, failedType, errorLog)) {

      try {
        metricsService.incrementAtomicInteger(FailedMsg.REQUEST_FAILED_OVER_MAXTRYNUM);
        // Surface the PS that finally caused the give-up — investigating "which PS killed
        // the job" used to require correlating multiple log lines by seqId. The per-type
        // limit is included separately so the line tells the truth when the limit isn't the
        // default maxTryNum (e.g. NETWORK_ERROR has limit = 2 * maxTryNum, FATAL has 1).
        LOG.error("request " + request + " failed after " + request.getContext().getTryCounter()
            + " tries (limit=" + tryLimitFor(failedType)
            + ", default maxTryNum=" + maxTryNum + ")"
            + ", lastServerId=" + request.getContext().getActualServerId()
            + ", lastLoc=" + request.getContext().getLocation()
            + ", lastErrorType=" + failedType
            + ", lastError=" + errorLog
            + ", notify to caller");
        FutureResult response = requestToResultMap.remove(request);
        if (response != null) {
          response.setExecuteError(errorLog);
        } else {
          LOG.warn("can not find response for request " + request);
        }
      } finally {
        // Notify error to user future result. requestError() now also evicts the bookkeeping
        // entries atomically (was a separate clear() call before); the pair was racy because
        // a parallel response on another partition could see "future failed but cache still
        // present" and try to merge into a doomed cache.
        PSAgentContext.get().getUserRequestAdapter().requestError(request.getUserRequestId(), errorLog);
      }
      return;
    }

    if (LOG.isInfoEnabled()) {
      LOG.info("[REQUEST-FAIL-TRACE][RETRY] reqKey=" + reqKey
          + ", userRequestId=" + request.getUserRequestId()
          + ", seqId=" + seqId
          + ", nextTry=" + (request.getContext().getTryCounter() + 1)
          + ", retryType=" + failedType
          + ", reason=" + errorLog);
    }

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
        metricsService.incrementAtomicInteger(FailedMsg.INVALID_REQUEST);
        LOG.error("unvalid request " + request);
        break;
    }
  }

  private void resetContext(Request request) {
    request.getContext().reset();
  }

  /**
   * Release every per-request resource that was acquired during the in-flight window without
   * scheduling a retry. Used by the FATAL response path in {@code Responser} where the caller
   * has already been notified via {@code futureResult.setExecuteException} and retrying is
   * not meaningful. Mirrors the cleanup that {@link #requestFailed} performs at its head,
   * deliberately skipping the retry-bookkeeping at its tail.
   */
  private void releaseInflightForFatal(int seqId, String errorLog) {
    Request request = seqIdToRequestMap.remove(seqId);
    if (request == null) {
      return;
    }
    releaseInflightForFatalOn(request, seqId, errorLog);
  }

  private void releaseInflightForFatalOn(Request request, int seqId, String errorLog) {
    String reqKey = "u" + request.getUserRequestId() + "#r" + request.hashCode();
    try {
      rpcContext.after(request.getContext().getServerId());
    } catch (Throwable ignore) {
      // serverInflightRPCCounters may not contain the id if the request was constructed
      // before psIds were known; defensively ignore
    }
    seqIdToSendCFMap.remove(seqId);
    returnChannel(request);
    returnBuffer(request);
    requestToResultMap.remove(request);
    metricsService.incrementAtomicInteger(FailedMsg.REQUEST_FAILED_OVER_MAXTRYNUM);
    if (LOG.isWarnEnabled()) {
      LOG.warn("[REQUEST-FAIL-TRACE][FATAL] reqKey=" + reqKey
          + ", userRequestId=" + request.getUserRequestId()
          + ", seqId=" + seqId
          + ", type=" + request.getType()
          + ", serverId=" + request.getContext().getServerId()
          + ", actualServerId=" + request.getContext().getActualServerId()
          + ", loc=" + request.getContext().getLocation()
          + ", reason=" + errorLog);
    }
  }

  private void psFailed(PSLocation psLoc) {
    // Remove all pending RPCS
    removePendingRPCS(psLoc);

    // Close all channel to this PS
    closeChannels(psLoc, "server-failed-event");
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
    closeChannels(psLoc, "unspecified");
  }

  private void closeChannels(PSLocation psLoc, String reason) {
    if (psLoc == null || psLoc.loc == null) {
      LOG.warn("[CHANNEL-REBUILD][POOL-CLOSE-SKIP] psLoc is null, reason=" + reason);
      return;
    }
    Location channelLoc = new Location(psLoc.loc.getIp(), psLoc.loc.getPort() + 1);
    if (LOG.isInfoEnabled()) {
      LOG.info("[CHANNEL-REBUILD][POOL-CLOSE] reason=" + reason + ", psLoc=" + psLoc
          + ", channelLoc=" + channelLoc);
    }
    channelManager.removeChannels(channelLoc);
  }

  private void putRequestSuccess(Request request) {
    try {
      dispatchMessageQueue.put(new RequestDispatchEvent(EventType.PUT_SUCCESS, request));
    } catch (Exception e) {
      metricsService.incrementAtomicInteger(FailedMsg.PUT_SUCCESS_FAILED);
      LOG.error("add PUT_SUCCESS event for request " + request + " failed, ", e);
    }
  }

  private void getRequestSuccess(Request request) {
    try {
      dispatchMessageQueue.put(new RequestDispatchEvent(EventType.GET_SUCCESS, request));
    } catch (Exception e) {
      metricsService.incrementAtomicInteger(FailedMsg.GET_SUCCESS_FAILED);
      LOG.error("add GET_SUCCESS event for request " + request + " failed, ", e);
    }
  }

  // getRequestNotReady() removed: never called anywhere, and EventType.GET_NOTREADY was not
  // handled by the dispatcher switch either. Pure dead code.

  private void getRequestFailed(Request request, ResponseType failedType, String errorLog) {
    try {
      dispatchMessageQueue
          .put(new RequestFailedEvent(EventType.GET_FAILED, request, failedType, errorLog));
      if (LOG.isInfoEnabled()) {
        String reqKey = "u" + request.getUserRequestId() + "#r" + request.hashCode();
        LOG.info("[REQUEST-FAIL-TRACE][ENQUEUE][GET] reqKey=" + reqKey
            + ", userRequestId=" + request.getUserRequestId()
            + ", try=" + request.getContext().getTryCounter()
            + ", failedType=" + failedType
            + ", reason=" + errorLog);
      }
    } catch (Exception e) {
      metricsService.incrementAtomicInteger(FailedMsg.GET_FAILED);
      LOG.error("add GET_FAILED event for request " + request + " failed, ", e);
    }
  }

  private void putRequestFailed(Request request, ResponseType failedType, String errorLog) {
    try {
      dispatchMessageQueue
          .put(new RequestFailedEvent(EventType.PUT_FAILED, request, failedType, errorLog));
      if (LOG.isInfoEnabled()) {
        String reqKey = "u" + request.getUserRequestId() + "#r" + request.hashCode();
        LOG.info("[REQUEST-FAIL-TRACE][ENQUEUE][PUT] reqKey=" + reqKey
            + ", userRequestId=" + request.getUserRequestId()
            + ", try=" + request.getContext().getTryCounter()
            + ", failedType=" + failedType
            + ", reason=" + errorLog);
      }
    } catch (Exception e) {
      metricsService.incrementAtomicInteger(FailedMsg.PUT_FAILED);
      LOG.error("add PUT_FAILED event for request " + request + " failed, ", e);
    }
  }


  /**
   * Compute the per-failure-type retry budget for a single RPC.
   *
   * <p>Historically every failure shared the same {@code maxTryNum} budget, which made two
   * very different things behave identically:</p>
   * <ul>
   *   <li>A transient {@code NETWORK_ERROR} caused by a single dead long-lived connection —
   *       the next retry already lands on a freshly-connected channel and almost always
   *       succeeds. These deserve a generous budget; failing the user-level request after
   *       only {@code maxTryNum} tries throws away an easy recovery.</li>
   *   <li>A {@code SERVER_HANDLE_FATAL} returned by the PS to say "this request will never
   *       succeed no matter how many times you try" (bad partition state, unsupported op,
   *       header parse failure). Retrying these is pure cost.</li>
   * </ul>
   *
   * <p>The new schedule:</p>
   * <ul>
   *   <li>{@code SERVER_HANDLE_FATAL} / {@code UNSUPPORT_REQUEST} / {@code PARSE_HEADER_FAILED}
   *       → limit 1 (give up immediately). The {@code Responser} FATAL branch already routes
   *       through {@code releaseInflightForFatal} and never reaches this function, but the
   *       {@code Requester.sendRequest} catch path and the timeout checker can still surface
   *       these via {@code requestFailed}, so the bound matters here too.</li>
   *   <li>{@code NETWORK_ERROR} / {@code CONNECT_REFUSED} → limit {@code 2 * maxTryNum},
   *       because the underlying dead channel is invalidated on the very same call and the
   *       next attempt typically succeeds.</li>
   *   <li>everything else → {@code maxTryNum} (legacy behaviour).</li>
   * </ul>
   */
  private boolean isOverTryLimit(Request request, ResponseType failedType, String errorLog) {
    request.getContext().setErrorLog(errorLog);
    return request.getContext().getTryCounter() >= tryLimitFor(failedType);
  }

  /**
   * Thin wrapper around {@link RetryPolicy#tryLimit} kept for backward compatibility with
   * the in-file error-log call sites. The real logic — and its UT — lives in {@link RetryPolicy}.
   */
  private int tryLimitFor(ResponseType failedType) {
    return retryPolicy.tryLimit(failedType);
  }

  private void refreshServerLocationSuccess(ParameterServerId serverId, boolean isUpdated) {
    try {
      dispatchMessageQueue.put(
          new RefreshServerLocationEvent(EventType.REFRESH_SERVER_LOCATION_SUCCESS, serverId,
              isUpdated));
    } catch (Exception e) {
      metricsService.incrementAtomicInteger(FailedMsg.REFRESH_SERVER_LOCATION_SUCCESS_FAILED);
      LOG.error("add REFRESH_SERVER_LOCATION_SUCCESS event failed, ", e);
    }
  }

  private void refreshServerLocationFailed(ParameterServerId serverId) {
    try {
      dispatchMessageQueue
          .put(new RefreshServerLocationEvent(EventType.REFRESH_SERVER_LOCATION_FAILED, serverId));
    } catch (Exception e) {
      metricsService.incrementAtomicInteger(FailedMsg.REFRESH_SERVER_LOCATION_FAILED);
      LOG.error("add REFRESH_SERVER_LOCATION_FAILED event failed, ", e);
    }
  }

  private void startGet() {
    try {
      dispatchMessageQueue.put(new DispatcherEvent(EventType.START_GET));
    } catch (Exception e) {
      metricsService.incrementAtomicInteger(FailedMsg.START_GET_FAILED);
      LOG.error("add START_GET event failed, ", e);
    }

  }

  private void startPut() {
    try {
      dispatchMessageQueue.put(new DispatcherEvent(EventType.START_PUT));
    } catch (Exception e) {
      metricsService.incrementAtomicInteger(FailedMsg.START_PUT_FAILED);
      LOG.error("add START_PUT event failed, ", e);
    }
  }

  private void returnBuffer(Request item) {
    ByteBuf buf = item.getContext().getSerializedData();
    if (buf != null && buf.refCnt() > 0) {
      try {
        buf.release();
      } catch (Throwable x) {
        metricsService.incrementAtomicInteger(FailedMsg.RELEASE_BUFFER_FAILED);
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
      metricsService.incrementAtomicInteger(FailedMsg.RETURN_CHANNEL_FAILED);
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
      metricsService.incrementAtomicInteger(FailedMsg.GET_PS_LOCATION_FAILED);
      LOG.error("get ps location from master failed.", e);
      return null;
    }

    if (location != null) {
      PSAgentContext.get().getLocationManager().setPsLocation(serverId, location);
    }
    return location;
  }

  // getGetUseTimes() removed along with the unused getUseTimes field.

  // Static so it can be constructed in unit tests without a MatrixTransportClient instance;
  // the class touches no outer-class state and the non-static form was a historical accident.
  static class SendResultKey {

    public final PSLocation psLoc;
    public final ChannelFuture cf;

    public SendResultKey(PSLocation psLoc, ChannelFuture cf) {
      this.psLoc = psLoc;
      this.cf = cf;
    }
  }

  /**
   * Result of {@link #matchSeqIdsForClosedChannel}: which seqIds need to be failed because
   * the netty {@code channel} they rode on has been observed inactive. Two disjoint groups
   * are returned because the call sites use slightly different error messages.
   */
  static final class ChannelMatchResult {
    final java.util.List<Integer> viaContextChannel;
    final java.util.List<Integer> viaSendCF;

    ChannelMatchResult(java.util.List<Integer> viaContextChannel,
        java.util.List<Integer> viaSendCF) {
      this.viaContextChannel = viaContextChannel;
      this.viaSendCF = viaSendCF;
    }
  }

  /**
   * Determine which {@code seqId}s should be terminated when {@code channel} goes inactive.
   * Pure function over the two bookkeeping maps so the two-pass logic is unit-testable
   * without standing up the dispatcher.
   *
   * <p><b>Why two passes.</b> A request's lifetime contains a window where neither map alone
   * uniquely identifies it as bound to {@code channel}:
   * <ul>
   *   <li>{@code request.context.channel} is set just before {@code writeAndFlush} and
   *       cleared by {@code returnChannel} in the write-listener — so it is non-null only
   *       during the brief "submitted but not yet flushed" + "flushed but listener
   *       not-yet-fired" window.</li>
   *   <li>{@code seqIdToSendCFMap} entry is created on the same {@code writeAndFlush} call
   *       and outlives the listener. It is the authoritative carrier identity for
   *       "request is in-flight and waiting for a response".</li>
   * </ul>
   * Combining both gives full coverage and the {@link #viaContextChannel} / {@link #viaSendCF}
   * split avoids double-failing the same seqId.
   */
  static ChannelMatchResult matchSeqIdsForClosedChannel(
      java.util.Map<Integer, Request> seqIdToRequestMap,
      java.util.Map<Integer, SendResultKey> seqIdToSendCFMap,
      Channel channel) {
    java.util.ArrayList<Integer> viaContextChannel = new java.util.ArrayList<>();
    java.util.HashSet<Integer> claimed = new java.util.HashSet<>();
    if (channel != null) {
      for (java.util.Map.Entry<Integer, Request> entry : seqIdToRequestMap.entrySet()) {
        NettyChannel ch = entry.getValue().getContext().getChannel();
        if (ch != null && ch.getChannel() != null && ch.getChannel().equals(channel)) {
          viaContextChannel.add(entry.getKey());
          claimed.add(entry.getKey());
        }
      }
    }
    java.util.ArrayList<Integer> viaSendCF = new java.util.ArrayList<>();
    if (channel != null) {
      for (java.util.Map.Entry<Integer, SendResultKey> e : seqIdToSendCFMap.entrySet()) {
        if (claimed.contains(e.getKey())) continue;
        SendResultKey srk = e.getValue();
        if (srk == null) continue;
        ChannelFuture cf = srk.cf;
        if (cf != null && cf.channel() == channel) {
          viaSendCF.add(e.getKey());
        }
      }
    }
    return new ChannelMatchResult(viaContextChannel, viaSendCF);
  }


  /**
   * Promote every {@link Request} in {@code from} whose server-id equals
   * {@code recoveredServerId} into {@code to}, regardless of remaining back-off. Returns
   * the count moved (0 if nothing matched).
   *
   * <p>Hoisted out as a package-private static helper so that the recovery-path logic
   * can be unit-tested without standing up a whole {@link MatrixTransportClient}. Every
   * input is passed in and there is no reliance on outer-class state.
   *
   * <p><b>Implementation note.</b> {@link Request#equals(Object)} is hard-coded to return
   * {@code false} — it is intentionally identity-only and the class only relies on the
   * AtomicInteger-based {@link Request#hashCode()} for hashed collections. We therefore
   * cannot use {@code from.removeAll(...)} (which goes through equals) or
   * {@code to.contains(...)}; the implementation uses a direct copy-then-rebuild scheme
   * driven by reference identity, which is the only thing the {@code Request} contract
   * actually supports.
   */
  static int forcePromoteForRecoveredPSImpl(java.util.PriorityQueue<Request> from,
      LinkedBlockingQueue<Request> to, ParameterServerId recoveredServerId) {
    if (from.isEmpty() || recoveredServerId == null) {
      return 0;
    }
    // Drain the entire heap into a snapshot, partition by reference into "matches the
    // recovered PS" vs. "leave alone", and re-insert the latter back into the heap so
    // the source ends up with exactly the unrelated requests. {@code PriorityQueue#clear}
    // + bulk re-insert is cheaper and simpler than per-element removal that would have to
    // contend with the broken equals contract.
    java.util.ArrayList<Request> survivors = new java.util.ArrayList<>(from.size());
    java.util.ArrayList<Request> moved = new java.util.ArrayList<>();
    Request item;
    while ((item = from.poll()) != null) {
      ParameterServerId itemSrv = item.getContext().getServerId();
      if (itemSrv != null && itemSrv.equals(recoveredServerId)) {
        moved.add(item);
      } else {
        survivors.add(item);
      }
    }
    if (!survivors.isEmpty()) {
      from.addAll(survivors);
    }
    if (moved.isEmpty()) {
      return 0;
    }
    to.addAll(moved);
    return moved.size();
  }

  /**
   * Per-PS counters tracking how many {@code getChannel} calls succeeded / failed / saw an
   * inactive channel since the last {@link #reset()}. Used by {@code checkChannelContext} to
   * decide when to recycle a PS's channel pool.
   *
   * <p>Implementation: {@link LongAdder} instead of {@code int + ReentrantLock}. The previous
   * implementation took a lock on every single channel borrow — i.e. on the hot path of
   * every PS RPC, by every {@code Requester} thread — purely to bump an int counter. Under
   * contention this can dominate the borrow cost. {@code LongAdder} is contention-striped
   * and avoids the lock entirely. We only read the values from the periodic check thread
   * (1Hz) and from log lines, so the slight tearing of {@code sum()} versus concurrent
   * increments is irrelevant; we'll see the latest counts on the next tick.</p>
   *
   * <p>{@code reset()} is not atomic across the three counters but the only consumer (the
   * periodic check) immediately starts a new window after reading, so any increment that
   * races with reset just folds into the next window — completely benign.</p>
   */
  // Static so it can be instantiated by unit tests without standing up a full PSAgent (the
  // class touches no outer-class state, the non-static inner-class form was an accident of
  // history). Also slightly cheaper: no synthetic outer-this field per instance.
  static class GetChannelContext {

    private final java.util.concurrent.atomic.LongAdder failedCounter =
        new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder successCounter =
        new java.util.concurrent.atomic.LongAdder();
    private final java.util.concurrent.atomic.LongAdder notactiveCounter =
        new java.util.concurrent.atomic.LongAdder();
    private volatile long lastCheckTs = System.currentTimeMillis();

    /**
     * Number of consecutive {@code checkChannelContext} ticks that observed
     * {@code inactive > 0} for this PS without any in-window success. Survives
     * {@link #reset()} on purpose: each tick resets the per-window counters but bumps or
     * clears this counter depending on whether the window was "all-bad" or not. Used by
     * {@code checkChannelContext} to detect a stuck PS — repeated tear-down + reconnect
     * with zero progress, which is the symptom of the "client churns reconnecting against
     * an RST-on-accept PS" pattern (e.g. PS process restarting / OOM-killed / migrated).
     */
    private volatile int consecutiveInactiveWindows = 0;

    public void getChannelSuccess() {
      successCounter.increment();
    }

    public void getChannelFailed() {
      failedCounter.increment();
    }

    public void channelNotactive() {
      notactiveCounter.increment();
    }

    public void reset() {
      failedCounter.reset();
      successCounter.reset();
      notactiveCounter.reset();
      lastCheckTs = System.currentTimeMillis();
    }

    /** Bump the cross-window stuck counter; called when the just-finished window had
     *  inactive > 0 AND no new success. Caller invokes {@link #reset()} immediately after. */
    public int incConsecutiveInactiveWindows() {
      return ++consecutiveInactiveWindows;
    }

    /** Clear the cross-window stuck counter; called when the just-finished window had at
     *  least one success (i.e. the PS made forward progress). */
    public void clearConsecutiveInactiveWindows() {
      consecutiveInactiveWindows = 0;
    }

    public int getConsecutiveInactiveWindows() {
      return consecutiveInactiveWindows;
    }

    public int getFailedCounter() {
      return saturatedToInt(failedCounter.sum());
    }

    public int getSuccessCounter() {
      return saturatedToInt(successCounter.sum());
    }

    public int getInactiveCounter() {
      return saturatedToInt(notactiveCounter.sum());
    }

    public long getLastCheckTs() {
      return lastCheckTs;
    }

    private int saturatedToInt(long v) {
      if (v > Integer.MAX_VALUE) return Integer.MAX_VALUE;
      if (v < Integer.MIN_VALUE) return Integer.MIN_VALUE;
      return (int) v;
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
   * The RPC request sender. Implemented as a {@link Runnable} (not a {@link Thread} subclass)
   * because all instances are submitted to {@code requestThreadPool} and never call
   * {@code start()} directly; the previous {@code extends Thread} encouraged the deprecated
   * {@code this.stop()} call in the failure handler, which is a no-op against the pool's
   * actual worker thread anyway.
   */
  class Requester implements Runnable {

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
        metricsService.incrementAtomicInteger(FailedMsg.SEND_REQUEST_IS_INTERRUPTED);
        LOG.error("send request " + request + " is interrupted");
      } catch (Throwable e) {
        metricsService.incrementAtomicInteger(FailedMsg.SEND_REQUEST_IS_FAILED);
        LOG.error("send request " + request + " failed, ", e);
        // Best-effort: release the inflight bookkeeping so this seqId doesn't leak.
        // Previously called Thread.stop() here, which (a) is deprecated and (b) has no
        // effect on the pool worker that actually ran this Runnable.
        try {
          requestFailed(seqId, ResponseType.UNKNOWN_ERROR,
              "send request threw: " + StringUtils.stringifyException(e));
        } catch (Throwable ignore) {
          // last-line defence; nothing else we can do here
        }
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
      request.getHeader().setTryCount(request.getContext().getTryCounter());

      // If location is null, means that the server is not ready
      if (psLoc.loc == null) {
        metricsService.incrementAtomicInteger(FailedMsg.REQUEST_SERVER_IS_NULL);
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

      request.rpcTimes.clientTimes.getChannelStart = System.currentTimeMillis();
      Channel channel;
      try {
        channel = getChannel(request, psLoc).getChannel();
      } catch (Throwable e) {
        if (!stopped.get()) {
          getChannelContext(psLoc).getChannelFailed();
          metricsService.incrementAtomicInteger(FailedMsg.GET_CHANNEL_FAILED);
          // The "channel is not active" path has already produced a throttled warn inside
          // getChannel(); avoid printing a second copy with a full stack trace here.
          boolean isInactive = (e instanceof IOException) && e.getMessage() != null
              && e.getMessage().contains("is not active");
          if (isInactive) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("get channel for " + psLoc.loc + " failed: " + e.getMessage());
            }
          } else {
            LOG.error("get channel for " + psLoc.loc + " failed ", e);
          }
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

      request.rpcTimes.clientTimes.getTokenStart = System.currentTimeMillis();

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
          metricsService.incrementAtomicInteger(FailedMsg.GET_TOKEN_FAILED);
          LOG.error("get token from PS " + psLoc.loc + " failed ", e);
          requestFailed(seqId, ResponseType.NETWORK_ERROR, StringUtils.stringifyException(e));
          return;
        }
      }

      //LOG.info("Send request " + request.getHeader());

      // Serialize the request
      startTs = System.currentTimeMillis();
      ByteBuf buffer;
      try {
        buffer = serializeRequest(request);
      } catch (Throwable e) {
        if (e instanceof OutOfMemoryError) {
          rpcContext.oom();
        }

        metricsService.incrementAtomicInteger(FailedMsg.SERIALIZE_REQUEST_FAILED);
        LOG.error("serialize request " + request + " failed ", e);
        requestFailed(seqId, ResponseType.OOM, StringUtils.stringifyException(e));
        return;
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("request with seqId=" + seqId + " serialize request use time " + (
            System.currentTimeMillis() - startTs));
      }

      // Send the request
      request.rpcTimes.clientTimes.sendRequestStart = System.currentTimeMillis();

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
        metricsService.incrementAtomicInteger(FailedMsg.GET_PARTITION_LOCATION_FAILED);
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
          metricsService.incrementAtomicInteger(FailedMsg.GET_PARTITION_LOCATION_FAILED);
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

      // If the borrowed channel is dead, take three actions before failing the request:
      //   1) Evict THIS specific channel from the pool synchronously, so the next concurrent
      //      borrow on the same pool cannot pick the same dead entry (root cause of the
      //      "N near-identical errors at the same millisecond" pattern in driver logs).
      //   2) After enough consecutive inactive observations, drop the whole pool — the most
      //      common failure mode (silent NAT/firewall reap or remote restart) takes out all
      //      long-lived connections at once.
      //   3) Throttle the log: at most one warn per PS per second, no stack trace.
      if (!channel.getChannel().isActive() || !channel.getChannel().isOpen()) {
        GetChannelContext ctx = getChannelContext(psLoc);
        ctx.channelNotactive();
        metricsService.incrementAtomicInteger(FailedMsg.CHANNEL_IS_NOT_ACTIVE);

        String reqKey = "u" + request.getUserRequestId() + "#r" + request.hashCode();

        // (1) Evict this dead channel right away.
        try {
          channelManager.removeChannel(channel);
          if (LOG.isInfoEnabled()) {
            LOG.info("[CHANNEL-REBUILD][EVICT-ONE] reqKey=" + reqKey
                + ", seqId=" + seqId
                + ", try=" + request.getContext().getTryCounter()
                + ", psLoc=" + psLoc
                + ", channel=" + channel
                + ", inactiveCounter=" + ctx.getInactiveCounter());
          }
        } catch (Throwable ignore) {
          // best-effort; pool checker will clean up later
          LOG.warn("[CHANNEL-REBUILD][EVICT-ONE-FAILED] reqKey=" + reqKey
              + ", seqId=" + seqId
              + ", try=" + request.getContext().getTryCounter()
              + ", psLoc=" + psLoc
              + ", channel=" + channel, ignore);
        }

        // (2) If too many concurrent failures, recycle the whole pool to this PS.
        if (ctx.getInactiveCounter() >= 3) {
          closeChannels(psLoc, "borrowed-channel-inactive-threshold");
          ctx.reset();
        }

        // (3) Throttled, single-line log.
        long now = System.currentTimeMillis();
        Long lastTs = psLocToLastInactiveLogTsMap.get(psLoc);
        if (lastTs == null || now - lastTs >= INACTIVE_LOG_THROTTLE_MS) {
          psLocToLastInactiveLogTsMap.put(psLoc, now);
          LOG.warn("channel " + channel + " to " + psLoc
              + " is not active (similar errors within " + INACTIVE_LOG_THROTTLE_MS
              + "ms are suppressed), reqKey=" + reqKey
              + ", seqId=" + seqId
              + ", try=" + request.getContext().getTryCounter()
              + ", active=" + channel.getChannel().isActive()
              + ", open=" + channel.getChannel().isOpen()
              + ", inactiveCounter=" + ctx.getInactiveCounter());
        }
        throw new IOException("channel " + channel + " is not active");
      }

      //request.getContext().setChannelPool(pool);
      request.getContext().setChannel(channel);
      return channel;
    }
  }

  private ByteBuf serializeRequest(Request request) {
    // Allocate the bytebuf and serialize the request
    request.rpcTimes.clientTimes.allocateRequestBufStart = System.currentTimeMillis();
    ByteBuf buffer = ByteBufUtils.newByteBuf(request.bufferLen());
    request.rpcTimes.clientTimes.serializeRequestStart = System.currentTimeMillis();

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
        metricsService.incrementAtomicInteger(FailedMsg.SEND_REQUEST_FAILED);
        LOG.error("send request " + seqId + " failed ", future.cause());
        requestFailed(seqId, ResponseType.NETWORK_ERROR,
            "send request failed " + future.cause().toString());
      } else {
        returnChannel(request);
      }
      request.rpcTimes.clientTimes.sendRequestOver = System.currentTimeMillis();
    }
  }


  public void handleResponse(SerializedMsgWithTime response) {
    responseThreadPool.execute(new Responser(response));
  }

  /**
   * Peek the first 4 bytes of a response frame as the {@code seqId} (matches
   * {@link com.tencent.angel.ps.server.data.response.ResponseHeader} wire layout) without
   * advancing the reader index. Returns {@code -1} if the buffer is too short or any
   * exception is thrown — the caller treats that as "unknown seqId, drop silently".
   *
   * <p>Hoisted out so {@link ResponserRejectedPolicy} can extract the seqId of a
   * to-be-dropped response and route it through {@link #requestFailed} instead of leaking
   * the inflight bookkeeping for that request.
   */
  static int peekSeqId(SerializedMsgWithTime msg) {
    if (msg == null || msg.msg == null) return -1;
    ByteBuf buf = msg.msg;
    try {
      if (buf.readableBytes() < 4) return -1;
      // Use getInt(idx) — no side effect on readerIndex, so the in-band path (if it ever
      // runs after a reject for any reason) still sees an intact frame.
      return buf.getInt(buf.readerIndex());
    } catch (Throwable ignore) {
      return -1;
    }
  }

  /**
   * Rejected-execution handler for {@link #responseThreadPool}.
   *
   * <p>Background: the historical policy was {@link java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy},
   * chosen because the caller is a netty IO thread and running the deserialize there back-
   * pressures the TCP receive window. But every {@code NioEventLoop} multiplexes many
   * channels, so a multi-second inline deserialize stalls every other channel sharing the
   * same loop. We have correlated that with mass channel-inactive bursts in production.
   *
   * <p>New behaviour: peek the {@code seqId} out of the rejected frame, fail that seqId as
   * {@link ResponseType#SERVER_IS_BUSY} (which the dispatcher will retry through the
   * normal back-off path), release the {@link ByteBuf} immediately, and return — the IO
   * thread is freed within microseconds. Net effect: back-pressure now travels through
   * the retry mechanism rather than through the IO loop.
   *
   * <p>If {@link AngelConf#ANGEL_MATRIXTRANSFER_CLIENT_RESPONSER_FAIL_FAST_ON_REJECT}
   * is set to false (regression hatch), we fall back to {@code CallerRunsPolicy}.
   */
  final class ResponserRejectedPolicy implements java.util.concurrent.RejectedExecutionHandler {
    private final boolean failFast;
    // Throttle the WARN log to at most one per second per process to avoid log spam under
    // sustained pool saturation; the per-seqId failure already gets propagated to the
    // caller, so a high-rate log line adds no diagnostic value.
    private volatile long lastWarnTs = 0L;
    private static final long WARN_THROTTLE_MS = 1000L;
    private final java.util.concurrent.atomic.LongAdder rejectedCounter =
        new java.util.concurrent.atomic.LongAdder();

    ResponserRejectedPolicy(boolean failFast) {
      this.failFast = failFast;
    }

    long getRejectedCount() {
      return rejectedCounter.sum();
    }

    @Override
    public void rejectedExecution(Runnable r,
        java.util.concurrent.ThreadPoolExecutor executor) {
      if (executor.isShutdown()) {
        // Pool is winding down; drop and release, the caller will get nothing back but the
        // adapter deadline watchdog plus user-side cancellation will tidy up.
        releaseRejected(r);
        return;
      }
      if (!failFast) {
        // Regression hatch: behave like CallerRunsPolicy.
        try {
          r.run();
        } catch (Throwable t) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("CallerRuns fallback for rejected Responser threw", t);
          }
        }
        return;
      }

      rejectedCounter.increment();
      // Extract the seqId before we release the buffer so the dispatcher can fail-fast
      // the matching inflight request. -1 means "couldn't parse" — drop silently; the
      // request will eventually be reaped by the user-request deadline watchdog.
      int seqId = -1;
      SerializedMsgWithTime msg = null;
      if (r instanceof Responser) {
        msg = ((Responser) r).msgWithTime;
        seqId = peekSeqId(msg);
      }

      try {
        long now = System.currentTimeMillis();
        if (now - lastWarnTs >= WARN_THROTTLE_MS) {
          lastWarnTs = now;
          LOG.warn("responseThreadPool saturated"
              + " (pool=" + executor.getPoolSize()
              + ", active=" + executor.getActiveCount()
              + ", queue=" + executor.getQueue().size()
              + "/" + (executor.getQueue().size() + executor.getQueue().remainingCapacity())
              + ", cumulativeRejected=" + rejectedCounter.sum()
              + "); failing seqId=" + seqId
              + " as SERVER_IS_BUSY to free the netty IO thread");
        }

        if (seqId >= 0) {
          try {
            requestFailed(seqId, ResponseType.SERVER_IS_BUSY,
                "responseThreadPool saturated; response dropped, request will be retried");
          } catch (Throwable t) {
            // Even if requestFailed throws (shouldn't), we MUST still release the buffer.
            LOG.error("requestFailed for rejected seqId=" + seqId + " threw", t);
          }
        }
      } finally {
        // Always release the response buffer; otherwise we leak direct memory on every reject.
        if (msg != null && msg.msg != null) {
          try {
            msg.msg.release();
          } catch (Throwable ignore) {
            // ByteBuf could already have refCnt==0 if some other path raced us; ignore.
          }
        }
      }
    }

    private void releaseRejected(Runnable r) {
      if (r instanceof Responser) {
        SerializedMsgWithTime m = ((Responser) r).msgWithTime;
        if (m != null && m.msg != null) {
          try {
            m.msg.release();
          } catch (Throwable ignore) {}
        }
      }
    }
  }

  /**
   * Holder for the active {@link ResponserRejectedPolicy} so tests and the metrics
   * subsystem can introspect the cumulative reject count. {@code volatile} because the
   * policy is wired up inside {@link #init()} which races with the constructor's
   * field-access ordering otherwise.
   */
  private volatile ResponserRejectedPolicy responserRejectedPolicy;

  /** @return cumulative number of response frames rejected by the responder pool since start. */
  public long getResponserRejectedCount() {
    return responserRejectedPolicy == null ? 0L : responserRejectedPolicy.getRejectedCount();
  }

  /**
   * RPC responses handler. Implemented as a {@link Runnable} (not a {@link Thread} subclass)
   * because all instances are submitted to {@code responseThreadPool}.
   */
  public class Responser implements Runnable {

    private SerializedMsgWithTime msgWithTime;

    public Responser(SerializedMsgWithTime msgWithTime) {
      this.msgWithTime = msgWithTime;
    }

    @Override
    public void run() {
      int seqId = 0;
      Request request = null;
      try {
        // Parse response header
        Response response = new Response();
        response.deserializeHeader(msgWithTime.msg);
        ResponseHeader header = response.getHeader();
        seqId = header.getSeqId();

        //LOG.info("ResponseHeader=" + header);

        // Atomically claim ownership of this seqId. Using remove() (instead of get()) means
        // that if a concurrent timeout / channel-close / FATAL path also tries to terminate
        // the same in-flight request, exactly one path wins and the loser sees null and
        // becomes a no-op. Combined with FutureResult's write-once guard this eliminates the
        // historical race where SUCCESS could be silently overwritten by a stale TIMEOUT (or
        // vice versa), and prevents responseHandler.handle from being invoked twice for the
        // same seqId.
        request = seqIdToRequestMap.remove(header.seqId);
        if(request == null) {
          // Another thread already terminated this request; nothing more to do.
          return;
        }

        // Set request data
        response.setRequestData(request.getData());

        // Parse response data
        response.deserializeData(msgWithTime.msg);

        request.rpcTimes.clientTimes.recvResponseStart = msgWithTime.ts;
        request.rpcTimes.clientTimes.deserializeResponseStart = System.currentTimeMillis();

        // TODO: for LDA, will be remove future
        FutureResult subResult = requestToResultMap.get(request);
        if(request.getType() == TransportMethod.GET_PSF && subResult != null) {
          GetUDFResponse getUDFResponse = new GetUDFResponse();
          getUDFResponse.deserialize(msgWithTime.msg);
          subResult.set(getUDFResponse.getPartResult());
        }

        if(request.getType() == TransportMethod.UPDATE_PSF && subResult != null) {
          UpdateUDFResponse updateUDFResponse = new UpdateUDFResponse();
          updateUDFResponse.deserialize(msgWithTime.msg);
          subResult.set(new VoidResult(com.tencent.angel.psagent.matrix.ResponseType.SUCCESS));
        }

        int userRequestId = request.getUserRequestId();

        // Update Server state
        handleServerState(request, header.state);

        if (request.getType() == TransportMethod.UPDATE_PSF || request.getType() == TransportMethod.GET_PSF) {
          request.rpcTimes.serverTimes = response.serverTimes;
          request.rpcTimes.clientTimes.handleResponseStart = System.currentTimeMillis();
        }

        //LOG.info("Handle request " + request.getHeader() + ", response " + response.getHeader());

        // Get user request and result cache
        UserRequestAdapter adapter = PSAgentContext.get().getUserRequestAdapter();
        UserRequest userRequest = adapter.getUserRequest(userRequestId);
        ResponseCache responseCache = adapter.getResponseCache(userRequestId);
        FutureResult futureResult = adapter.getFutureResult(userRequestId);

        //LOG.info("userRequest=" + userRequest + ", responseCache=" + responseCache + ", futureResult=" + futureResult);

        if(userRequest == null || responseCache == null || futureResult == null) {
          // The user-level request was already torn down (typically because an earlier
          // partition's failure cascaded a clear() on the whole userRequestId). Release the
          // in-flight resources we hold for THIS partition so they don't leak; the user-side
          // future has already been completed by whoever cleared the adapter entry.
          releaseInflightForFatalOn(request, seqId,
              "userRequest entry already cleared, treating as fatal");
          return;
        }

        //LOG.info("responseCache " + responseCache.getExpectedResponseNum());

        if (header.getResponseType() != ResponseType.SUCCESS && LOG.isWarnEnabled()) {
          String reqKey = "u" + request.getUserRequestId() + "#r" + request.hashCode();
          LOG.warn("[REQUEST-FAIL-TRACE][RESP] reqKey=" + reqKey
              + ", userRequestId=" + request.getUserRequestId()
              + ", seqId=" + seqId
              + ", type=" + request.getType()
              + ", serverId=" + request.getContext().getServerId()
              + ", actualServerId=" + request.getContext().getActualServerId()
              + ", loc=" + request.getContext().getLocation()
              + ", respType=" + header.getResponseType()
              + ", detail=" + header.detail);
        }

        switch (header.getResponseType()) {

          case SUCCESS: {
            if (request.getType() == TransportMethod.UPDATE_PSF || request.getType() == TransportMethod.GET_PSF) {
              metricsService.logRPCStageTime(request, request.rpcTimes);
            }

            // Get response handler
            TransportMethod method = TransportMethod.valueOf(header.methodId);
            Handler responseHandler = responseHanders.get(method);
            if(responseHandler == null) {
              LOG.error("Can not find handler for method " + method);
              return;
            }

            synchronized (responseCache) {
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
            }

            // Handle success: use the by-request flavour because we have already removed
            // this seqId from seqIdToRequestMap at the top of run(); the legacy by-seqId
            // form would now see null and skip the cleanup.
            requestSuccessOn(request, seqId);
            break;
          }

          case OOM:
          case SERVER_IS_BUSY:
          case SERIALIZE_RESPONSE_FAILED: {
            // Server is busy now, retry
            requestFailedOn(request, seqId, ResponseType.SERVER_IS_BUSY, header.detail);
            break;
          }

          case SERVER_NOT_READY: {
            // Server is not ready, retry
            requestFailedOn(request, seqId, ResponseType.SERVER_NOT_READY, header.detail);
            break;
          }

          case PARSE_HEADER_FAILED:
          case UNSUPPORT_REQUEST:
          case SERVER_HANDLE_FAILED:
          case SERVER_HANDLE_FATAL: {
            // Handle failed, notify the caller AND release the in-flight resources so that
            // rpcContext.inflight counters, seqIdToSendCFMap, channel and buffer are all
            // released. Previously only futureResult.setExecuteException + adapter.clear
            // were called, leaving the seqId stuck in seqIdToRequestMap and the per-server
            // inflight counter permanently incremented -- a slow leak that eventually
            // starved the dispatcher's flow-control. FATAL means "retry is pointless",
            // so we don't go through the normal requestFailed retry path.
            futureResult.setExecuteException(new ExecutionException(
                new AngelException("Handle request failed " + header.detail)));
            adapter.clear(request.getUserRequestId());
            releaseInflightForFatalOn(request, seqId, "fatal: " + header.detail);
            break;
          }
        }
      } catch (Throwable x) {
        metricsService.incrementAtomicInteger(FailedMsg.HANDLER_RPC_RESPONSE_FAILED);
        LOG.error("handler rpc response failed ", x);
        if (request != null) {
          // We already own this request (removed it from seqIdToRequestMap at the top of
          // run()), so use the by-request termination form.
          if (x instanceof OutOfMemoryError) {
            rpcContext.oom();
            requestFailedOn(request, seqId, ResponseType.OOM, StringUtils.stringifyException(x));
          } else {
            requestFailedOn(request, seqId, ResponseType.UNKNOWN_ERROR,
                StringUtils.stringifyException(x));
          }
        } else {
          // Header parse failed before we could even read seqId; nothing left to clean up.
          if (x instanceof OutOfMemoryError) {
            rpcContext.oom();
          }
        }
      } finally {
        metricsService.handleFailedMsg(request);
        msgWithTime.msg.release();
      }
    }

    private void handleServerState(Request request, ServerState state) {
      if (state != null) {
        psIdToStateMap.put(request.getContext().getActualServerId(), state);
      }
    }
  }
}
