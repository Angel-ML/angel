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


package com.tencent.angel.ps.server.data;

import com.tencent.angel.common.AngelThreadFactory;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.server.data.handler.BasicStreamUpdateHandler;
import com.tencent.angel.ps.server.data.handler.GetPartHandler;
import com.tencent.angel.ps.server.data.handler.GetRowHandler;
import com.tencent.angel.ps.server.data.handler.GetRowsHandler;
import com.tencent.angel.ps.server.data.handler.Handler;
import com.tencent.angel.ps.server.data.handler.IndexGetRowHandler;
import com.tencent.angel.ps.server.data.handler.IndexGetRowsHandler;
import com.tencent.angel.ps.server.data.handler.PSFGetHandler;
import com.tencent.angel.ps.server.data.handler.PSFUpdateHandler;
import com.tencent.angel.ps.server.data.handler.StreamIndexGetRowHandler;
import com.tencent.angel.ps.server.data.profiling.MatrixTransportServerMetrics;
import com.tencent.angel.ps.server.data.profiling.ServerTimes;
import com.tencent.angel.ps.server.data.request.RequestData;
import com.tencent.angel.ps.server.data.request.RequestHeader;
import com.tencent.angel.ps.server.data.response.Response;
import com.tencent.angel.ps.server.data.response.ResponseData;
import com.tencent.angel.ps.server.data.response.ResponseHeader;
import com.tencent.angel.ps.server.data.response.ResponseType;
import com.tencent.angel.utils.ByteBufUtils;
import com.tencent.angel.utils.StringUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * RPC worker pool for matrix transformation
 */
public class WorkerPool {

  private static final Log LOG = LogFactory.getLog(WorkerPool.class);

  /**
   * Threshold (in handled-element count, taken from {@link RequestHeader#handleElemNum})
   * below which a PSF request is processed <em>inline</em> on the netty IO thread instead
   * of being dispatched to the RPCWorker pool. Saves one queue-and-wakeup per request,
   * which is meaningful for tiny PSFs (e.g. dst-side PPR batches of 1-2 seeds) but is
   * dangerous for general workloads because anything blocking on the IO thread freezes
   * every other channel pinned to the same event loop.
   *
   * <p>Defaults to 0 (== always async, historical behaviour). Operators who profile and
   * confirm their PSFs return in microseconds at small element counts may opt in by
   * setting {@code angel.matrixtransfer.server.sync.threshold} to a positive value.
   */
  private final int syncThreshold;

  /**
   * PS context
   */
  private final PSContext context;

  /**
   * Use unlock rpc handler or not
   */
  private final boolean useUnlockRPC;

  /**
   * Use direct buffer for Netty
   */
  private final boolean useDirectorBuffer;

  /**
   * Use netty buffer pool
   */
  private final boolean usePool;

  /**
   * Use independent sender and workers
   */
  private final boolean useAyncHandler;

  /**
   * Get router from local cache or Master, false means get from Master
   */
  private final boolean disableRouterCache;

  private final boolean useStreamHandler;

  /**
   * RPC worker pool
   */
  private volatile ExecutorService workerPool;

  /**
   * Matrix partition to rpc message queue map
   */
  private final ConcurrentHashMap<MatrixPartition, PartitionHandler> lockFreeHandlers;

  /**
   * Netty server running context
   */
  private final RunningContext runningContext;

  /**
   * Use independent worker pool for rpc handler
   */
  private final boolean useInDepWorkers;

  /**
   * Is stop services
   */
  private final AtomicBoolean stopped = new AtomicBoolean(false);

  private final Map<Integer, Handler> methodIdToHandler;

  private final boolean metricsEnable;

  /**
   * Per-method RPC counters. Always non-null (process-wide singleton); when disabled
   * by config we leave it unstarted (so {@link MatrixTransportServerMetrics#start} is
   * a no-op) but still record into it — record() is allocation-free and lock-free,
   * so the cost is below noise on every realistic workload, and keeping the same
   * call shape removes a branch from the hot path.
   *
   * <p>The PSF handlers ({@code PSFGetHandler} / {@code PSFUpdateHandler}) reach the
   * same instance via {@link MatrixTransportServerMetrics#getInstance()} so they can
   * record per-PSF-class breakdown without a back-pointer to {@link WorkerPool}.
   */
  private final MatrixTransportServerMetrics metrics = MatrixTransportServerMetrics.getInstance();
  private final AtomicBoolean firstRpcLogged = new AtomicBoolean(false);

  /**
   * Create a WorkerPool
   *
   * @param context PS context
   */
  public WorkerPool(PSContext context, RunningContext runningContext) {
    this.context = context;
    this.runningContext = runningContext;

    Configuration conf = context.getConf();
    useDirectorBuffer = conf.getBoolean(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEDIRECTBUFFER,
        AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEDIRECTBUFFER);

    usePool = conf.getBoolean(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEPOOL,
        AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEPOOL);

    ByteBufUtils.useDirect = useDirectorBuffer;
    ByteBufUtils.usePool = usePool;

    useAyncHandler = conf.getBoolean(AngelConf.ANGEL_MATRIXTRANSFER_SERVER_USE_ASYNC_HANDLER,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_USE_ASYNC_HANDLER);

    // Default 0 == always async (historical behaviour). The previous code had a 1000
    // hard-coded constant whose if/else branches both returned true – i.e. the threshold
    // was effectively dead code. Exposing it as a knob lets operators opt in for tiny
    // PSFs without changing the default.
    syncThreshold = conf.getInt(AngelConf.ANGEL_MATRIXTRANSFER_SERVER_SYNC_THRESHOLD,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_SYNC_THRESHOLD);

    int partReplicNum = conf.getInt(AngelConf.ANGEL_PS_HA_REPLICATION_NUMBER,
        AngelConf.DEFAULT_ANGEL_PS_HA_REPLICATION_NUMBER);

    useUnlockRPC = conf.getBoolean(AngelConf.ANGEL_MATRIXTRANSFER_SERVER_USE_UNLOCK_RPC,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_USE_UNLOCK_RPC);

    lockFreeHandlers = new ConcurrentHashMap<>();

    useInDepWorkers = conf.getBoolean(AngelConf.ANGEL_PS_USE_INDEPENDENT_WORKER_POOL,
        AngelConf.DEFAULT_ANGEL_PS_USE_INDEPENDENT_WORKER_POOL);

    useStreamHandler = conf
        .getBoolean(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_USE_STREAM_HANDLER,
            AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_USE_STREAM_HANDLER);

    metricsEnable = context.getMetricsEnable();

    if (partReplicNum > 1) {
      disableRouterCache = true;
    } else {
      disableRouterCache = false;
    }

    methodIdToHandler = new HashMap<>();
    registerHanders();
  }

  private void registerHanders() {
    methodIdToHandler.put(TransportMethod.GET_ROWSPLIT.getMethodId(), new GetRowHandler(context));
    methodIdToHandler.put(TransportMethod.GET_ROWSSPLIT.getMethodId(), new GetRowsHandler(context));
    if (useStreamHandler) {
      methodIdToHandler
          .put(TransportMethod.INDEX_GET_ROW.getMethodId(), new StreamIndexGetRowHandler(context));
    } else {
      methodIdToHandler
          .put(TransportMethod.INDEX_GET_ROW.getMethodId(), new IndexGetRowHandler(context));
    }

    methodIdToHandler
        .put(TransportMethod.INDEX_GET_ROWS.getMethodId(), new IndexGetRowsHandler(context));
    methodIdToHandler.put(TransportMethod.GET_PSF.getMethodId(), new PSFGetHandler(context));
    methodIdToHandler.put(TransportMethod.UPDATE_PSF.getMethodId(), new PSFUpdateHandler(context));
    methodIdToHandler.put(TransportMethod.GET_PART.getMethodId(), new GetPartHandler(context));
    methodIdToHandler
        .put(TransportMethod.UPDATE.getMethodId(), new BasicStreamUpdateHandler(context));
  }

  /**
   * Init
   */
  public void init() {
  }

  /**
   * Start workers
   */
  public void start() {
    Configuration conf = context.getConf();
    int configuredWorkerPoolSize = conf.getInt(
        AngelConf.ANGEL_MATRIXTRANSFER_SERVER_WORKER_POOL_SIZE,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_WORKER_POOL_SIZE);
    int nettyEventGroupThreads = conf.getInt(
        AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_EVENTGROUP_THREADNUM,
        AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_EVENTGROUP_THREADNUM);

    workerPool = useInDepWorkers
        ? Executors.newFixedThreadPool(configuredWorkerPoolSize, new AngelThreadFactory("RPCWorker"))
        : null;

    LOG.info("[PS-THREAD-POOL] rpc-handler mode="
        + (useInDepWorkers ? "independent-worker-pool" : "netty-eventloop")
        + ", configuredWorkerPoolSize=" + configuredWorkerPoolSize
        + ", nettyEventGroupThreads=" + nettyEventGroupThreads
        + ", useAsyncHandler=" + useAyncHandler
        + ", useUnlockRPC=" + useUnlockRPC);

    // Expose the CURRENT dispatch backlog to the metrics printer, so every
    // [PS-RPC-METRICS] tick carries both the per-window queue-wait latency and the
    // instantaneous queue depth / worker saturation.
    metrics.setQueueDepthSampler(new MatrixTransportServerMetrics.QueueDepthSampler() {
      @Override
      public long queuedTasks() {
        if (useUnlockRPC) {
          long sum = 0;
          for (PartitionHandler handler : lockFreeHandlers.values()) {
            sum += handler.queueSize();
          }
          return sum;
        }
        ExecutorService pool = workerPool;
        if (pool instanceof ThreadPoolExecutor) {
          return ((ThreadPoolExecutor) pool).getQueue().size();
        }
        // netty-eventloop mode: the pending task count is per event loop, not exposed
        // throughExecutorService, so we report "unknown" rather than a wrong 0.
        return -1;
      }

      @Override
      public int activeWorkers() {
        if (useUnlockRPC) {
          return -1;
        }
        ExecutorService pool = workerPool;
        return pool instanceof ThreadPoolExecutor
            ? ((ThreadPoolExecutor) pool).getActiveCount() : -1;
      }

      @Override
      public int poolSize() {
        if (useUnlockRPC) {
          return lockFreeHandlers.size();
        }
        ExecutorService pool = workerPool;
        return pool instanceof ThreadPoolExecutor
            ? ((ThreadPoolExecutor) pool).getPoolSize() : -1;
      }

      @Override
      public String mode() {
        if (useUnlockRPC) {
          return "per-partition-queue";
        }
        return useInDepWorkers ? "independent-worker-pool" : "netty-eventloop";
      }
    });


    boolean metricsEnabled = conf.getBoolean(
        AngelConf.ANGEL_MATRIXTRANSFER_SERVER_METRICS_ENABLE,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_METRICS_ENABLE);
    if (metricsEnabled) {
      // PSF-class breakdown: enable BEFORE starting the scheduler so the first tick
      // already sees PSF buckets populated. The PSF handlers (PSFGetHandler /
      // PSFUpdateHandler) check via the volatile psfBuckets field whether to record;
      // starting it here makes them effective immediately.
      boolean psfEnabled = conf.getBoolean(
          AngelConf.ANGEL_MATRIXTRANSFER_SERVER_METRICS_PSF_ENABLE,
          AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_METRICS_PSF_ENABLE);
      if (psfEnabled) {
        int topN = conf.getInt(
            AngelConf.ANGEL_MATRIXTRANSFER_SERVER_METRICS_PSF_TOP_N,
            AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_METRICS_PSF_TOP_N);
        metrics.enablePsfBreakdown(topN);
      }

      long intervalMs = conf.getLong(
          AngelConf.ANGEL_MATRIXTRANSFER_SERVER_METRICS_LOG_INTERVAL_MS,
          AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_METRICS_LOG_INTERVAL_MS);
      metrics.start(intervalMs);
    }
  }

  /**
   * Stop
   */
  public void stop() {
    if (stopped.compareAndSet(false, true)) {
      metrics.stop();
      if (workerPool != null) {
        workerPool.shutdownNow();
      }

      for (PartitionHandler handler : lockFreeHandlers.values()) {
        handler.interrupt();
      }

      lockFreeHandlers.clear();
    }
  }

  /**
   * Handler RPC request
   *
   * @param ctx channel context
   * @param msg request
   */
  public void handlerRequest(ChannelHandlerContext ctx, Object msg) {
    // Receive request
    ServerTimes serverTimes = new ServerTimes();
    serverTimes.receive = System.currentTimeMillis();
    // Nano-resolution origin for the queue-wait metric: `receive` is millis and would
    // truncate most dispatch waits to 0. PS-local only, never serialized.
    serverTimes.receiveNanos = System.nanoTime();

    // Deserialize head
    ByteBuf in = (ByteBuf) msg;
    RequestHeader header = parseHeader(in);

    // Get method type
    TransportMethod method = TransportMethod.typeIdToTypeMap.get(header.methodId);

    // Check method
    if (method == null) {
      throw new AngelException("Can not support method " + header.methodId);
    }

    if (firstRpcLogged.compareAndSet(false, true)) {
      LOG.info("[PS-SERVER-READY][FIRST-RPC] method=" + method
          + ", seqId=" + header.seqId
          + ", clientId=" + header.clientId
          + ", local=" + ctx.channel().localAddress()
          + ", remote=" + ctx.channel().remoteAddress());
    }


    // Check if data request
    if (isDataRequest(method)) {
      runningContext.before(header.clientId, header.seqId);
      runningContext.relaseToken(header.clientId, header.token);
    }

    try {
      // Use async handler or not
      boolean useAsync = needAsync(method, header);

      if (useAsync) {
        // Mark as queued BEFORE the hand-off: the worker may start before this method
        // returns, and it reads the flag when recording the queue-wait metric.
        serverTimes.queued = true;
        if (useUnlockRPC) {
          handleLockFree(header, in, ctx, serverTimes);
        } else {
          getWorker(ctx).execute(new Processor(header, in, ctx, serverTimes));
        }
      } else {
        handle(header, in, ctx, serverTimes);
      }
    } catch (Throwable x) {
      LOG.error("handle rpc " + header + " failed ", x);
      throw x;
    }
  }

  private RequestHeader parseHeader(ByteBuf in) {
    RequestHeader header = new RequestHeader();
    header.deserialize(in);
    return header;
  }

  private ExecutorService getWorker(ChannelHandlerContext ctx) {
    if (useInDepWorkers) {
      return workerPool;
    } else {
      return ctx.executor();
    }
  }

  public ExecutorService getWorkerPool() {
    return workerPool;
  }

  private void handleLockFree(RequestHeader header, ByteBuf in, ChannelHandlerContext ctx, ServerTimes serverTimes) {
    MatrixPartition mp = new MatrixPartition(header.matrixId, header.partId);

    // Get and init the queue
    ChannelHandlerContextMsg task = new ChannelHandlerContextMsg(header, in, ctx, serverTimes);
    PartitionHandler handler = lockFreeHandlers.get(mp);
    if (handler == null) {
      synchronized (this) {
        handler = lockFreeHandlers.get(mp);
        if (handler == null) {
          handler = new PartitionHandler(mp);
          handler.setName("Partition-Hanlder-" + mp);
          handler.start();
          lockFreeHandlers.put(mp, handler);
        }
      }
    }

    try {
      handler.add(task);
    } catch (Throwable e) {
      LOG.error("Add task to Partition handler for " + mp + " failed ", e);
      throw new RuntimeException(
          "Add task to Partition handler for " + mp + " failed " + StringUtils
              .stringifyException(e));
    }
  }

  class PartitionHandler extends Thread {

    private final MatrixPartition mp;
    private final LinkedBlockingQueue<ChannelHandlerContextMsg> taskQueue;

    public PartitionHandler(MatrixPartition mp) {
      this.mp = mp;
      this.taskQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void run() {
      while (!stopped.get() && !Thread.interrupted()) {
        try {
          ChannelHandlerContextMsg task = taskQueue.take();
          handle(task.getHeader(), task.getMessage(), task.getCtx(), task.getServerTimes());
        } catch (InterruptedException e) {
          if (stopped.get() || Thread.interrupted()) {
            LOG.warn("Partition handler for " + mp + " stopped");
          } else {
            LOG.error("Take task from partitioned task queue failed ", e);
          }
        }
      }
    }

    public void add(ChannelHandlerContextMsg task) throws InterruptedException {
      taskQueue.put(task);
    }

    /**
     * @return number of requests currently waiting in this partition's queue
     */
    public int queueSize() {
      return taskQueue.size();
    }
  }

  private boolean isDataRequest(TransportMethod method) {
    switch (method) {
      case GET_ROWSPLIT:
      case GET_ROWSSPLIT:
      case GET_PSF:
      case GET_PART:
      case PUT_PART:
      case UPDATE:
      case INDEX_GET_ROW:
      case INDEX_GET_ROWS:
      case UPDATE_PSF:
      case CHECKPOINT:
        return true;

      default:
        return false;
    }
  }

  /**
   * Response processor
   */
  class Processor implements Runnable {

    /**
     * Request header(meta)
     */
    private RequestHeader header;

    /**
     * Request
     */
    private ByteBuf message;

    /**
     * Channel context
     */
    private ChannelHandlerContext ctx;

    private final ServerTimes serverTimes;

    Processor(RequestHeader header, ByteBuf message, ChannelHandlerContext ctx, ServerTimes serverTimes) {
      this.header = header;
      this.message = message;
      this.ctx = ctx;
      this.serverTimes = serverTimes;
    }

    @Override
    public void run() {
      try {
        handle(header, message, ctx, serverTimes);
        message = null;
        ctx = null;
      } catch (Throwable x) {
        LOG.error("handle request failed ", x);
      }
    }
  }

  /**
   * Check the request is a simple request: has short processing time
   *
   * @param method request type
   * @return true means it's a complex request, use async mode
   */
  private boolean needAsync(TransportMethod method, RequestHeader header) {
    if (!useAyncHandler) {
      return false;
    }

    // syncThreshold == 0 means "always async" (the historical, safe default). When
    // configured > 0, requests below the threshold are processed inline on the IO thread
    // to save one queue-and-wakeup hop. Risk: a PSF that turns out to be CPU-heavy at
    // small element counts will block the netty event loop and freeze every other
    // channel sharing it. Only enable after profiling.
    if (syncThreshold > 0 && header.handleElemNum <= syncThreshold) {
      return false;
    }
    return true;
  }

  private String requestToString(int clientId, int seqId) {
    return "clientId=" + clientId + "/seqId=" + seqId;
  }

  private void sendResult(RequestHeader header, ChannelHandlerContext ctx, Object result, ServerTimes serverTimes) {
    if(metricsEnable) {
      serverTimes.update((ByteBuf)result);
    }

    ctx.writeAndFlush(result).addListener(new GenericFutureListener<Future<? super Void>>() {
      @Override
      public void operationComplete(Future<? super Void> future) {
        if (isDataRequest(TransportMethod.valueOf(header.methodId))) {
          if (future.isSuccess()) {
            context.getRunningContext().increaseNormal();
          } else {
            LOG.error("send response of request " + requestToString(header.clientId, header.seqId)
                + " failed ");
            context.getRunningContext().increaseNetworkError();
          }
          context.getRunningContext().after(header.clientId, header.seqId);
        }
      }
    });
  }

  /**
   * Handle rpc
   *
   * @param requestHeader request meta data
   * @param in request data
   * @param ctx netty channel context
   */
  private void handle(RequestHeader requestHeader, ByteBuf in, ChannelHandlerContext ctx, ServerTimes serverTimes) {
    // Start handler rpc
    serverTimes.handleStart = System.currentTimeMillis();
    final long handleStartNanos = System.nanoTime();
    // Dispatch-queue wait: how long this request sat between "netty handed us the bytes"
    // and "a worker picked it up". Recorded right here, before any handling work, so it
    // is a pure queueing measure. Zero for the inline (sync) path.
    if (serverTimes.receiveNanos > 0) {
      metrics.recordQueueWait(requestHeader.methodId,
          handleStartNanos - serverTimes.receiveNanos, serverTimes.queued);
    }
    // E2E time origin: when netty handed us the bytes (set in handlerRequest entry).
    // Falls back to handleStart if receive-time wasn't recorded (defensive only).
    final long e2eOriginMs = serverTimes.receive > 0 ? serverTimes.receive : serverTimes.handleStart;

    ResponseData responseData = null;
    ResponseHeader responseHeader;
    ServerState state = runningContext.getState();
    long requestBytes = 0L;
    boolean success = false;

    //LOG.info("Request header = " + requestHeader);

    if (state == ServerState.BUSY) {
      responseHeader = new ResponseHeader(requestHeader.seqId, requestHeader.methodId,
          ServerState.BUSY, ResponseType.SERVER_IS_BUSY, "server is busy now, retry later");
    } else {
      // Idle, handle the request
      // Parse head success
      Handler handler = methodIdToHandler.get(requestHeader.methodId);
      // Handle request
      if (handler != null) {
        try {
          // Deserialize request
          serverTimes.deserializeRequestStart = System.currentTimeMillis();
          // Use remaining buffer size as request size to support pipeline-style PSF update
          // params whose deserialize path may not consume all payload bytes immediately.
          long requestBufSize = in.readableBytes();
          ByteBufSerdeUtils.deserializeBoolean(in);
          RequestData request = handler.parseRequest(in);
          request.requestSize = requestBufSize;
          requestBytes = requestBufSize;
          runningContext.increaseRequestDataSize(requestBufSize);

          // Handle
          serverTimes.partExeStart = System.currentTimeMillis();
          responseData = handler.handle(requestHeader, request);
          runningContext.increaseRequestDataSize(responseData.getDataLen());
          responseHeader = new ResponseHeader(requestHeader.seqId, requestHeader.methodId, state,
              ResponseType.SUCCESS);
          success = true;
        } catch (Throwable e) {
          // Handle error, generate response
          responseHeader = new ResponseHeader(requestHeader.seqId, requestHeader.methodId, state,
              ResponseType.SERVER_HANDLE_FAILED, StringUtils.stringifyException(e));
        }
      } else {
        responseHeader = new ResponseHeader(requestHeader.seqId, requestHeader.methodId, state,
            ResponseType.UNSUPPORT_REQUEST);
      }
    }

    // Per-method counters: record handle latency and request size now (before response
    // serialise). Response size is added below once we know the serialised byte count.
    // Recording is allocation-free; nanos chosen so sub-ms PSFs still resolve cleanly.
    final long handleNanos = System.nanoTime() - handleStartNanos;
    final long e2eNanos = (System.currentTimeMillis() - e2eOriginMs) * 1_000_000L;

    // Release the input buffer
    ByteBufUtils.releaseBuf(in);

    // Create response
    Response response;
    if(metricsEnable) {
      response = new Response(responseHeader, responseData, serverTimes);
    } else {
      response = new Response(responseHeader, responseData, null);
    }

    //LOG.info("Response header = " + responseHeader);

    // Serialize the response
    // 2. Serialize the response
    ByteBuf out = ByteBufUtils.serializeResponse(response, context.isUseDirectBuffer(), serverTimes);
    long responseBytes = out == null ? 0L : out.readableBytes();

    // Per-method counters – record once per RPC. Done here (after response serialise)
    // so we can include the response wire size; the handle latency was captured before
    // serialise so it does not include serialisation cost. Lock-free, safe on hot path.
    metrics.record(requestHeader.methodId, handleNanos, e2eNanos,
        requestBytes, responseBytes, success);

    // Send the serialized response
    if (out != null) {
      serverTimes.sendResponseStart = System.currentTimeMillis();
      serverTimes.waitChannelStart = serverTimes.sendResponseStart;
      sendResult(requestHeader, ctx, out, serverTimes);
      return;
    } else {
      runningContext.after(requestHeader.clientId, requestHeader.seqId);
      return;
    }
  }

  private void oom() {
    context.getRunningContext().oom();
  }


}

