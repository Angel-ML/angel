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
import com.tencent.angel.exception.InvalidParameterException;
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
import com.tencent.angel.ps.server.data.handler.StreamIndexGetRowsHandler;
import com.tencent.angel.ps.server.data.request.GetUDFRequest;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * RPC worker pool for matrix transformation
 */
public class WorkerPool {

  private static final Log LOG = LogFactory.getLog(WorkerPool.class);
  public static final AtomicInteger total = new AtomicInteger(0);
  public static final AtomicInteger normal = new AtomicInteger(0);
  public static final AtomicInteger oom = new AtomicInteger(0);
  public static final AtomicInteger network = new AtomicInteger(0);
  public static final AtomicInteger channelInUseCounter = new AtomicInteger(0);
  public static final AtomicInteger unknown = new AtomicInteger(0);
  public static final AtomicLong requestSize = new AtomicLong(0);
  private static final int syncThreshold = 1000;

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

    int partReplicNum = conf.getInt(AngelConf.ANGEL_PS_HA_REPLICATION_NUMBER,
        AngelConf.DEFAULT_ANGEL_PS_HA_REPLICATION_NUMBER);

    useUnlockRPC = conf.getBoolean(AngelConf.ANGEL_MATRIXTRANSFER_SERVER_USE_UNLOCK_RPC,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_USE_UNLOCK_RPC);

    lockFreeHandlers = new ConcurrentHashMap<>();

    useInDepWorkers = conf.getBoolean(AngelConf.ANGEL_PS_USE_INDEPENDENT_WORKER_POOL,
        AngelConf.DEFAULT_ANGEL_PS_USE_INDEPENDENT_WORKER_POOL);

    useStreamHandler = conf.getBoolean(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_USE_STREAM_HANDLER,
        AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_USE_STREAM_HANDLER);

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
    if(useStreamHandler) {
      methodIdToHandler.put(TransportMethod.INDEX_GET_ROW.getMethodId(), new StreamIndexGetRowHandler(context));
    } else {
      methodIdToHandler.put(TransportMethod.INDEX_GET_ROW.getMethodId(), new IndexGetRowHandler(context));
    }

    methodIdToHandler.put(TransportMethod.INDEX_GET_ROWS.getMethodId(), new IndexGetRowsHandler(context));
    methodIdToHandler.put(TransportMethod.GET_PSF.getMethodId(), new PSFGetHandler(context));
    methodIdToHandler.put(TransportMethod.UPDATE_PSF.getMethodId(), new PSFUpdateHandler(context));
    methodIdToHandler.put(TransportMethod.GET_PART.getMethodId(), new GetPartHandler(context));
    methodIdToHandler.put(TransportMethod.UPDATE.getMethodId(), new BasicStreamUpdateHandler(context));
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
    workerPool = useInDepWorkers ?
        Executors.newFixedThreadPool(context.getConf()
                .getInt(AngelConf.ANGEL_MATRIXTRANSFER_SERVER_WORKER_POOL_SIZE,
                    AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_WORKER_POOL_SIZE),
            new AngelThreadFactory("RPCWorker")) :
        null;
  }

  /**
   * Stop
   */
  public void stop() {
    if(stopped.compareAndSet(false, true)) {
      if (workerPool != null) {
        workerPool.shutdownNow();
      }

      for(PartitionHandler handler: lockFreeHandlers.values()) {
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
    // Deserialize head
    ByteBuf in = (ByteBuf) msg;
    RequestHeader header = parseHeader(in);

    // Get method type
    TransportMethod method = TransportMethod.typeIdToTypeMap.get(header.methodId);

    // Check method
    if(method == null) {
      throw new AngelException("Can not support method " + header.methodId);
    }

    // Check if data request
    if (isDataRequest(method)) {
      total.incrementAndGet();
      runningContext.before(header.clientId, header.seqId);
      runningContext.relaseToken(header.clientId, header.token);
    }

    try {
      // Use async handler or not
      boolean useAsync = needAsync(method, header);

      if (useAsync) {
        if (useUnlockRPC) {
          handleLockFree(header, in, ctx);
        } else {
          getWorker(ctx).execute(new Processor(header, in, ctx));
        }
      } else {
        handle(header, in, ctx);
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

  private void handleLockFree(RequestHeader header, ByteBuf in, ChannelHandlerContext ctx) {
    MatrixPartition mp = new MatrixPartition(header.matrixId, header.partId);

    // Get and init the queue
    ChannelHandlerContextMsg task = new ChannelHandlerContextMsg(header, in, ctx);
    PartitionHandler handler = lockFreeHandlers.get(mp);
    if (handler == null) {
      synchronized (this) {
        handler = lockFreeHandlers.get(mp);
        if(handler == null) {
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
      throw new RuntimeException("Add task to Partition handler for " + mp + " failed " + StringUtils.stringifyException(e));
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
      while(!stopped.get() && !Thread.interrupted()) {
        try {
          ChannelHandlerContextMsg task = taskQueue.take();
          handle(task.getHeader(), task.getMessage(), task.getCtx());
        } catch (InterruptedException e) {
          if(stopped.get() || Thread.interrupted()) {
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

    Processor(RequestHeader header, ByteBuf message, ChannelHandlerContext ctx) {
      this.header = header;
      this.message = message;
      this.ctx = ctx;
    }

    @Override
    public void run() {
      try {
        handle(header, message, ctx);
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

    if(header.handleElemNum > syncThreshold) {
      return true;
    } else {
      return true;
    }
  }

  private String requestToString(int clientId, int seqId) {
    return "clientId=" + clientId + "/seqId=" + seqId;
  }

  private void sendResult(RequestHeader header, ChannelHandlerContext ctx, Object result) {

    ctx.writeAndFlush(result).addListener(new GenericFutureListener<Future<? super Void>>() {
      @Override
      public void operationComplete(Future<? super Void> future) {
        if (isDataRequest(TransportMethod.valueOf(header.methodId))) {
          if (future.isSuccess()) {
            normal.incrementAndGet();
          } else {
            LOG.error("send response of request " + requestToString(header.clientId, header.seqId) + " failed ");
            network.incrementAndGet();
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
  private void handle(RequestHeader requestHeader, ByteBuf in, ChannelHandlerContext ctx) {
    ResponseData responseData = null;
    ResponseHeader responseHeader;
    ServerState state = runningContext.getState();

    //LOG.info("Request header = " + requestHeader);

    if (state == ServerState.BUSY) {
      responseHeader = new ResponseHeader(requestHeader.seqId, requestHeader.methodId,
          ServerState.BUSY, ResponseType.SERVER_IS_BUSY, "server is busy now, retry later");
    } else {
      // Idle, handle the request
      // Parse head success
      Handler handler = methodIdToHandler.get(requestHeader.methodId);
      // Handle request
      if(handler != null) {
        try {
          ByteBufSerdeUtils.deserializeBoolean(in);
          RequestData request = handler.parseRequest(in);
          requestSize.addAndGet(request.requestSize);
          responseData = handler.handle(requestHeader, request);
          requestSize.addAndGet(responseData.bufferLen());
          responseHeader = new ResponseHeader(requestHeader.seqId, requestHeader.methodId, state, ResponseType.SUCCESS);
        } catch (Throwable e) {
          // Handle error, generate response
          responseHeader = new ResponseHeader(requestHeader.seqId, requestHeader.methodId, state, ResponseType.SERVER_HANDLE_FAILED, StringUtils.stringifyException(e));
        }
      } else {
        responseHeader = new ResponseHeader(requestHeader.seqId, requestHeader.methodId, state, ResponseType.UNSUPPORT_REQUEST);
      }
    }

    // Release the input buffer
    ByteBufUtils.releaseBuf(in);

    // Create response
    Response response = new Response(responseHeader, responseData);

    //LOG.info("Response header = " + responseHeader);

    // Serialize the response
    // 2. Serialize the response
    ByteBuf out = ByteBufUtils.serializeResponse(response, context.isUseDirectBuffer());

    // Send the serialized response
    if (out != null) {
      sendResult(requestHeader, ctx, out);
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

