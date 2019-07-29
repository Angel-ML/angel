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

import com.google.protobuf.ServiceException;
import com.tencent.angel.PartitionKey;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.WaitLockTimeOutException;
import com.tencent.angel.ml.matrix.PartitionLocation;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.server.data.request.*;
import com.tencent.angel.ps.server.data.response.*;
import com.tencent.angel.ps.storage.matrix.PartitionState;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerBasicTypeRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.utils.ByteBufUtils;
import com.tencent.angel.utils.StringUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
  private static final int syncThreshold = 10000;
  /**
   * Channel use state
   */
  private final ConcurrentHashMap<ChannelHandlerContext, AtomicBoolean> channelStates;

  /**
   * PS context
   */
  private final PSContext context;

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

  /**
   * RPC worker pool
   */
  private volatile ExecutorService workerPool;

  /**
   * Response sender pool
   */
  private volatile ExecutorService senderPool;

  /**
   * Netty server running context
   */
  private final RunningContext runningContext;

  /**
   * Create a WorkerPool
   *
   * @param context PS context
   */
  public WorkerPool(PSContext context, RunningContext runningContext) {
    this.context = context;
    this.runningContext = runningContext;
    channelStates = new ConcurrentHashMap<>();
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
    if (partReplicNum > 1) {
      disableRouterCache = true;
    } else {
      disableRouterCache = false;
    }
  }

  /**
   * Init
   *
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public void init() throws IllegalAccessException, InstantiationException {
  }

  /**
   * Start workers
   */
  public void start() {
    senderPool = useAyncHandler ?
      Executors.newFixedThreadPool(context.getConf()
        .getInt(AngelConf.ANGEL_MATRIXTRANSFER_SERVER_SENDER_POOL_SIZE,
          AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_SENDER_POOL_SIZE)) :
      null;

    workerPool = useAyncHandler ?
      Executors.newFixedThreadPool(context.getConf()
        .getInt(AngelConf.ANGEL_MATRIXTRANSFER_SERVER_WORKER_POOL_SIZE,
          AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_WORKER_POOL_SIZE)) :
      null;
  }

  /**
   * Stop
   */
  public void stop() {
    if (senderPool != null) {
      senderPool.shutdownNow();
    }

    if (workerPool != null) {
      workerPool.shutdownNow();
    }
  }

  /**
   * Register a Netty channel
   *
   * @param channel Netty channel
   */
  public void registerChannel(ChannelHandlerContext channel) {
    channelStates.put(channel, new AtomicBoolean(false));
  }

  /**
   * Unregister a Netty channel
   *
   * @param channel Netty channel
   */
  public void unregisterChannel(ChannelHandlerContext channel) {
    channelStates.remove(channel);
  }

  /**
   * Handler RPC request
   *
   * @param ctx channel context
   * @param msg request
   */
  public void handlerRequest(ChannelHandlerContext ctx, Object msg) {
    ByteBuf in = (ByteBuf) msg;
    int clientId = in.readInt();
    int token = in.readInt();
    int seqId = in.readInt();
    int methodId = in.readInt();
    TransportMethod method = TransportMethod.typeIdToTypeMap.get(methodId);

    if (isDataRequest(method)) {
      total.incrementAndGet();
      runningContext.before(clientId, seqId);
      runningContext.relaseToken(clientId, token);
    }

    boolean useAsync = needAsync(method, in);
    in.resetReaderIndex();
    if (useAsync) {
      workerPool.execute(new Processor((ByteBuf) msg, ctx));
    } else {
      handle(ctx, msg, true);
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
  class Processor extends Thread {
    /**
     * Request
     */
    private ByteBuf message;

    /**
     * Channel context
     */
    private ChannelHandlerContext ctx;

    Processor(ByteBuf message, ChannelHandlerContext ctx) {
      this.message = message;
      this.ctx = ctx;
    }

    @Override public void run() {
      handle(ctx, message, false);
      message = null;
      ctx = null;
    }
  }


  /**
   * Response sender
   */
  class Sender extends Thread {
    /**
     * Client ID
     */
    private int clientId;

    /**
     * Request seq id
     */
    private int seqId;

    /**
     * Request type
     */
    private TransportMethod method;

    /**
     * Response
     */
    private Object result;

    /**
     * Channel context
     */
    private ChannelHandlerContext ctx;

    public String uuid;

    Sender(int clientId, int seqId, TransportMethod method, ChannelHandlerContext ctx,
      Object result) {
      this.clientId = clientId;
      this.seqId = seqId;
      this.method = method;
      this.result = result;
      this.ctx = ctx;
    }

    @Override public void run() {
      send(clientId, seqId, method, ctx, result);
      result = null;
      ctx = null;
    }
  }

  /**
   * Check the request is a simple request: has short processing time
   *
   * @param method request type
   * @return true means it's a complex request, use async mode
   */
  private boolean needAsync(TransportMethod method, ByteBuf in) {
    if (!useAyncHandler) {
      return false;
    }

    if (method == TransportMethod.GET_CLOCKS || method == TransportMethod.UPDATE_CLOCK) {
      return false;
    } else if (method == TransportMethod.INDEX_GET_ROW || method == TransportMethod.INDEX_GET_ROWS
      || method == TransportMethod.UPDATE) {
      PartitionRequest request = new PartitionRequest();
      request.deserialize(in);
      if (request.getHandleElemNum() <= syncThreshold) {
        return false;
      }
    }

    return true;
  }

  /**
   * Send back the result
   *
   * @param clientId PSClient id
   * @param seqId    RPC seq id
   * @param ctx      channel context
   * @param result   rpc result
   */
  private void send(int clientId, int seqId, TransportMethod method, ChannelHandlerContext ctx,
    Object result) {
    Channel ch = ctx.channel();
    try {
      AtomicBoolean channelInUse = channelStates.get(ctx);
      if (channelInUse == null) {
        LOG.error("send response of request " + requestToString(clientId, seqId)
          + ", but channel is unregistered");
        if (isDataRequest(method)) {
          channelInUseCounter.incrementAndGet();
          runningContext.after(clientId, seqId);
          ((ByteBuf) result).release();
        }
        return;
      }
      long startTs = System.currentTimeMillis();
      while (true) {
        if (channelInUse.compareAndSet(false, true)) {
          ctx.writeAndFlush(result).addListener(new GenericFutureListener<Future<? super Void>>() {
            @Override public void operationComplete(Future<? super Void> future) throws Exception {
              if (isDataRequest(method)) {
                if (future.isSuccess()) {
                  normal.incrementAndGet();
                } else {
                  //                  LOG.error("send response of request " + requestToString(clientId, seqId) + " failed ");
                  network.incrementAndGet();
                }
                context.getRunningContext().after(clientId, seqId);
              }
            }
          });
          channelInUse.set(false);
          return;
        }
        Thread.sleep(10);
      }
    } catch (Throwable ex) {
      //      LOG.error("send response of request failed, request seqId=" + seqId + ", channel=" + ch, ex);
      unknown.incrementAndGet();
    }
  }

  private String requestToString(int clientId, int seqId) {
    return "clientId=" + clientId + "/seqId=" + seqId;
  }

  /**
   * Send the rpc result
   *
   * @param ctx     channel context
   * @param result  rpc result
   * @param useSync true means send it directly, false means send it use sender
   */
  private void sendResult(int clientId, int seqId, TransportMethod method,
    ChannelHandlerContext ctx, Object result, boolean useSync) {
    if (!useSync && useAyncHandler) {
      senderPool.execute(new Sender(clientId, seqId, method, ctx, result));
    } else {
      send(clientId, seqId, method, ctx, result);
    }
  }

  /**
   * Handle the request
   *
   * @param ctx     channel context
   * @param msg     rpc request
   * @param useSync true means handle it directly, false means handle it use Processor
   */
  private void handle(ChannelHandlerContext ctx, Object msg, boolean useSync) {
    ByteBuf in = (ByteBuf) msg;
    int clientId = in.readInt();
    int tokenNum = in.readInt();
    int seqId = in.readInt();
    int methodId = in.readInt();
    TransportMethod method = TransportMethod.typeIdToTypeMap.get(methodId);
    Response response = null;
    ByteBuf out = null;
    if (method == TransportMethod.INDEX_GET_ROW) {
      try {
        out = handleIndexGetRow(clientId, seqId, in);
      } catch (Throwable ex) {
        LOG.error("handler index get row failed ", ex);
      } finally {
        // Release the input buffer
        if (in.refCnt() > 0) {
          in.release();
        }
      }
    } else if (method == TransportMethod.INDEX_GET_ROWS) {
      try {
        out = handleIndexGetRows(clientId, seqId, in);
      } catch (Throwable ex) {
        LOG.error("handler index get row failed ", ex);
      } finally {
        // Release the input buffer
        if (in.refCnt() > 0) {
          in.release();
        }
      }
    } else {
      // 1. handle the rpc, get the response
      try {
        response = handleRPC(clientId, seqId, in, method);
      } catch (Throwable ex) {
        LOG.error("handler rpc failed ", ex);
      } finally {
        // Release the input buffer
        if (in.refCnt() > 0) {
          in.release();
        }
      }

      // 2. Serialize the response
      if (response != null) {
        try {
          out = serializeResponse(seqId, response);
        } catch (Throwable ex) {
          LOG.error("serialize response falied ", ex);
        }
      }
    }

    // Send the serialized response
    if (out != null) {
      sendResult(clientId, seqId, method, ctx, out, useSync);
    } else {
      runningContext.after(clientId, seqId);
      return;
    }
  }

  private ByteBuf handleIndexGetRow(int clientId, int seqId, ByteBuf in) throws Throwable {

    ServerState state = runningContext.getState();
    IndexPartGetRowResponse result;
    if (state == ServerState.BUSY) {
      result =
        new IndexPartGetRowResponse(ResponseType.SERVER_IS_BUSY, "server is busy now, retry later");
      result.setState(ServerState.BUSY);
    } else {
      IndexPartGetRowRequest request = new IndexPartGetRowRequest();
      request.deserialize(in);
      PartitionKey partKey = request.getPartKey();

      ServerBasicTypeRow row = (ServerBasicTypeRow)context.getMatrixStorageManager()
              .getRow(request.getMatrixId(), request.getRowId(), partKey.getPartitionId());
      IndexType indexType = IndexType.valueOf(in.readInt());
      ValueType valueType = getValueType(row.getRowType());
      int size = in.readInt();
      result = new IndexPartGetRowResponse(ResponseType.SUCCESS);
      result.setState(state);

      ByteBuf resultBuf = null;
      try {
        resultBuf = allocResultBuf(4 + result.bufferLen() + 4 + size * getValueSize(valueType));
      } catch (Throwable x) {
        LOG.error("allocate result buffer for request " + TransportMethod.INDEX_GET_ROW + " failed ", x);
        result.setResponseType(ResponseType.SERVER_IS_BUSY);
        result.setState(ServerState.BUSY);
        result.setDetail(StringUtils.stringifyException(x));

        // Exception happened
        ByteBuf out = null;
        try {
          out = serializeResponse(seqId, result);
        } catch (Throwable ex) {
          LOG.error("serialize response failed ", ex);
        }
        return out;
      }

      try {
        // write seq id
        resultBuf.writeInt(seqId);

        // Just serialize the head
        result.serialize(resultBuf);
        resultBuf.writeInt(valueType.getTypeId());
        resultBuf.writeInt(size);
        if (request.getFunc() == null) {
          row.startRead();
          row.indexGet(indexType, size, in, resultBuf, null);
          row.endRead();
        } else {
          row.startWrite();
          row.indexGet(indexType, size, in, resultBuf, request.getFunc());
          row.endWrite();
        }
        return resultBuf;
      } catch (WaitLockTimeOutException  | OutOfMemoryError x) {
        LOG.error("handle request " + TransportMethod.INDEX_GET_ROW + " failed ", x);
        resultBuf.release();

        result.setResponseType(ResponseType.SERVER_HANDLE_FAILED);
        result.setDetail(StringUtils.stringifyException(x));
      } catch (Throwable x) {
        LOG.error("handle request " + TransportMethod.INDEX_GET_ROW + " failed ", x);
        resultBuf.release();

        result.setResponseType(ResponseType.SERVER_HANDLE_FATAL);
        result.setDetail(StringUtils.stringifyException(x));
      }
    }

    // Exception happened
    ByteBuf out = null;
    try {
      out = serializeResponse(seqId, result);
    } catch (Throwable ex) {
      LOG.error("serialize response failed ", ex);
    }
    return out;
  }

  private ByteBuf handleIndexGetRows(int clientId, int seqId, ByteBuf in) throws Throwable {
    ServerState state = runningContext.getState();
    IndexPartGetRowsResponse result;
    if (state == ServerState.BUSY) {
      result = new IndexPartGetRowsResponse(ResponseType.SERVER_IS_BUSY,
        "server is busy now, retry later");
      result.setState(ServerState.BUSY);
    } else {
      IndexPartGetRowsRequest request = new IndexPartGetRowsRequest();
      request.deserialize(in);
      PartitionKey partKey = request.getPartKey();
      IndexType indexType = IndexType.valueOf(in.readInt());
      int colNum = in.readInt();
      List<Integer> rowIds = request.getRowIds();
      int rowNum = rowIds.size();

      ValueType valueType = getValueType(
        context.getMatrixMetaManager().getMatrixMeta(request.getMatrixId()).getRowType());
      result = new IndexPartGetRowsResponse(ResponseType.SUCCESS);
      result.setState(state);

      // Allocate result buffer
      ByteBuf resultBuf = null;
      try {
        resultBuf =
                allocResultBuf(4 + result.bufferLen() + 4 + colNum * rowNum * getValueSize(valueType));
      } catch (Throwable x) {
        LOG.error("allocate result buffer for request " + TransportMethod.INDEX_GET_ROWS + " failed ", x);
        result.setResponseType(ResponseType.SERVER_IS_BUSY);
        result.setState(ServerState.BUSY);
        result.setDetail(StringUtils.stringifyException(x));
        resultBuf.release();

        // Exception happened
        ByteBuf out = null;
        try {
          out = serializeResponse(seqId, result);
        } catch (Throwable ex) {
          LOG.error("serialize response failed ", ex);
        }
        return out;
      }

      try {
        resultBuf.writeInt(seqId);

        // Just serialize the head
        result.serialize(resultBuf);
        resultBuf.writeInt(valueType.getTypeId());
        resultBuf.writeInt(rowNum);
        resultBuf.writeInt(colNum);
        int markPos = in.readerIndex();
        for (int i = 0; i < rowNum; i++) {
          in.readerIndex(markPos);
          ServerBasicTypeRow row = (ServerBasicTypeRow)context.getMatrixStorageManager()
            .getRow(request.getMatrixId(), rowIds.get(i), partKey.getPartitionId());
          resultBuf.writeInt(rowIds.get(i));
          if(request.getFunc() == null) {
            row.startRead();
            row.indexGet(indexType, colNum, in, resultBuf, null);
            row.endRead();
          } else {
            row.startWrite();
            row.indexGet(indexType, colNum, in, resultBuf, request.getFunc());
            row.endWrite();
          }
        }
        return resultBuf;
      } catch (WaitLockTimeOutException | OutOfMemoryError x) {
        LOG.error("handle request " + TransportMethod.INDEX_GET_ROWS + " failed ", x);
        resultBuf.release();

        result.setResponseType(ResponseType.SERVER_HANDLE_FAILED);
        result.setDetail(StringUtils.stringifyException(x));
      } catch (Throwable x) {
        LOG.error("handle request  " + TransportMethod.INDEX_GET_ROWS + " failed ", x);
        resultBuf.release();

        result.setResponseType(ResponseType.SERVER_HANDLE_FATAL);
        result.setDetail(StringUtils.stringifyException(x));
      }
    }

    // Exception happened
    ByteBuf out = null;
    try {
      out = serializeResponse(seqId, result);
    } catch (Throwable ex) {
      LOG.error("serialize response falied ", ex);
    }
    return out;
  }

  private int getValueSize(ValueType valueType) {
    if (valueType == ValueType.DOUBLE || valueType == ValueType.LONG) {
      return 8;
    } else {
      return 4;
    }
  }

  private ValueType getValueType(RowType rowType) {
    switch (rowType) {
      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE:
      case T_DOUBLE_DENSE_COMPONENT:
      case T_DOUBLE_SPARSE_COMPONENT:
      case T_DOUBLE_SPARSE_LONGKEY:
      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
      case T_DOUBLE_DENSE_LONGKEY_COMPONENT:
        return ValueType.DOUBLE;

      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE:
      case T_FLOAT_DENSE_COMPONENT:
      case T_FLOAT_SPARSE_COMPONENT:
      case T_FLOAT_SPARSE_LONGKEY:
      case T_FLOAT_SPARSE_LONGKEY_COMPONENT:
      case T_FLOAT_DENSE_LONGKEY_COMPONENT:
        return ValueType.FLOAT;

      case T_INT_DENSE:
      case T_INT_SPARSE:
      case T_INT_DENSE_COMPONENT:
      case T_INT_SPARSE_COMPONENT:
      case T_INT_SPARSE_LONGKEY:
      case T_INT_SPARSE_LONGKEY_COMPONENT:
      case T_INT_DENSE_LONGKEY_COMPONENT:
        return ValueType.INT;

      case T_LONG_DENSE:
      case T_LONG_SPARSE:
      case T_LONG_DENSE_COMPONENT:
      case T_LONG_SPARSE_COMPONENT:
      case T_LONG_SPARSE_LONGKEY:
      case T_LONG_SPARSE_LONGKEY_COMPONENT:
      case T_LONG_DENSE_LONGKEY_COMPONENT:
        return ValueType.LONG;

      default:
        return ValueType.DOUBLE;
    }
  }

  private Response handleRPC(int clientId, int seqId, ByteBuf in, TransportMethod method) {
    Response result;
    ServerState state = runningContext.getState();
    String log = "server is busy now, retry later";
    long startTs = System.currentTimeMillis();
    switch (method) {
      case GET_ROWSPLIT: {
        if (state == ServerState.BUSY) {
          result = new GetRowSplitResponse(ResponseType.SERVER_IS_BUSY, log);
        } else {
          GetRowSplitRequest request = new GetRowSplitRequest();
          request.deserialize(in);
          result = getRowSplit(request);
        }
        break;
      }

      case GET_ROWSSPLIT: {
        if (state == ServerState.BUSY) {
          result = new GetRowsSplitResponse(ResponseType.SERVER_IS_BUSY, log);
        } else {
          GetRowsSplitRequest request = new GetRowsSplitRequest();
          request.deserialize(in);
          result = getRowsSplit(request);
        }
        break;
      }

      case GET_PART: {
        if (state == ServerState.BUSY) {
          result = new GetPartitionResponse(ResponseType.SERVER_IS_BUSY, log);
        } else {
          GetPartitionRequest request = new GetPartitionRequest();
          request.deserialize(in);
          result = getPartition(request);
        }
        break;
      }

      case UPDATE: {
        if (state == ServerState.BUSY) {
          result = new UpdateResponse(ResponseType.SERVER_IS_BUSY, log);
        } else {
          UpdateRequest request = new UpdateRequest();
          request.deserialize(in);
          result = update(request, in);
        }
        break;
      }

      case GET_CLOCKS: {
        GetClocksRequest request = new GetClocksRequest();
        request.deserialize(in);
        result = getClocks(request);
        break;
      }

      case GET_STATE: {
        GetStateRequest request = new GetStateRequest();
        request.deserialize(in);
        result = new GetStateResponse(ResponseType.SUCCESS);
        break;
      }

      case UPDATE_PSF: {
        if (state == ServerState.BUSY) {
          result = new UpdaterResponse(ResponseType.SERVER_IS_BUSY, log);
        } else {
          UpdaterRequest request = new UpdaterRequest();
          try {
            request.deserialize(in);
            result = update(request, in);
          } catch (Throwable x) {
            result = new UpdaterResponse(ResponseType.SERVER_HANDLE_FATAL, StringUtils.stringifyException(x));
          }
        }
        break;
      }

      case GET_PSF: {
        if (state == ServerState.BUSY) {
          result = new GetUDFResponse(ResponseType.SERVER_IS_BUSY, log);
        } else {
          GetUDFRequest request = new GetUDFRequest();
          try {
            request.deserialize(in);
            result = getSplit(request);
          } catch (Throwable x) {
            result = new GetUDFResponse(ResponseType.SERVER_HANDLE_FATAL, StringUtils.stringifyException(x));
          }
        }
        break;
      }

      case RECOVER_PART: {
        RecoverPartRequest request = new RecoverPartRequest();
        request.deserialize(in);
        result = recoverPart(request);
        break;
      }

      case UPDATE_CLOCK: {
        UpdateClockRequest request = new UpdateClockRequest();
        request.deserialize(in);
        result = updateClock(request);
        break;
      }

      case CHECKPOINT: {
        CheckpointPSRequest request = new CheckpointPSRequest();
        request.deserialize(in);
        result = checkpoint(request);
        break;
      }

      default:
        throw new UnsupportedOperationException("Unknown RPC type " + method);
    }

    if (state == ServerState.BUSY) {
      LOG.info("Hanle request " + requestToString(clientId, seqId) + " Server is BUSY now ");
      //runningContext.printToken();
    }
    result.setState(state);
    LOG.debug("handle request " + seqId + " use time=" + (System.currentTimeMillis() - startTs));
    return result;
  }

  private Response checkpoint(CheckpointPSRequest request) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("checkpoint request=" + request);
    }

    try {
      context.getSnapshotDumper().checkpoint(request.getMatrixId());
      return new CheckpointPSResponse(ResponseType.SUCCESS, "");
    } catch (Throwable e) {
      String detail = StringUtils.stringifyException(e);
      LOG.error("checkpoint failed", e);
      return new CheckpointPSResponse(ResponseType.SERVER_HANDLE_FAILED, detail);
    }
  }

  private ByteBuf serializeResponse(int seqId, Response response) {
    ByteBuf buf = null;
    try {
      buf = allocResultBuf(response);
      buf.writeInt(seqId);
      response.serialize(buf);
    } catch (Throwable x) {
      LOG.error("serialize response failed ", x);
      if (buf != null) {
        buf.release();
      }

      if (response.getResponseType() == ResponseType.SUCCESS) {
        // Reset the response and allocate buffer again
        response.setResponseType(ResponseType.SERVER_IS_BUSY);
        response.setDetail("can not serialize the response");
        response.clear();

        buf = allocResultBuf(response);
        buf.writeInt(seqId);
        response.serialize(buf);
      } else {
        throw x;
      }
    }
    return buf;
  }

  /**
   * Is the PS the master ps for a partition
   *
   * @param partLoc the stored pss for the location
   * @return true mean the PS is the master ps
   */
  private boolean isPartMasterPs(PartitionLocation partLoc) {
    return !partLoc.psLocs.isEmpty() && partLoc.psLocs.get(0).psId
      .equals(context.getPSAttemptId().getPsId());
  }

  /**
   * Get from the partition use PSF
   *
   * @param request request
   * @return response contain the get result
   */
  private GetUDFResponse getSplit(GetUDFRequest request) {
    try {
      Class<? extends GetFunc> funcClass =
        (Class<? extends GetFunc>) Class.forName(request.getGetFuncClass());
      Constructor<? extends GetFunc> constructor = funcClass.getConstructor();
      constructor.setAccessible(true);
      GetFunc func = constructor.newInstance();
      func.setPsContext(context);
      PartitionGetResult partResult = func.partitionGet(request.getPartParam());
      return new GetUDFResponse(ResponseType.SUCCESS, partResult);
    } catch (WaitLockTimeOutException | OutOfMemoryError e) {
      String log = "get udf request " + request + " failed " + StringUtils.stringifyException(e);
      LOG.error(log, e);
      return new GetUDFResponse(ResponseType.SERVER_IS_BUSY, log);
    } catch (Throwable e) {
      String log = "get udf request " + request + " failed " + StringUtils.stringifyException(e);
      LOG.fatal(log, e);
      return new GetUDFResponse(ResponseType.SERVER_HANDLE_FATAL, log);
    }
  }

  private ByteBuf allocResultBuf(Response response) {
    return allocResultBuf(4 + response.bufferLen());
  }

  private ByteBuf allocResultBuf(int size) {
    ByteBuf buf;
    try {
      buf = ByteBufUtils.newByteBuf(size, useDirectorBuffer);
    } catch (Throwable x) {
      LOG.error("allocate buf with size = " + size + " failed ", x);
      oom();
      throw x;
    }
    return buf;
    /*
    try {
      buf = ByteBufUtils.newByteBuf(size, useDirectorBuffer);
    } catch (Throwable x) {
      oom();
      LOG.error("allocate result buffer for response " + response + " failed ", x);
      if (response.getResponseType() == ResponseType.SUCCESS) {
        response.setResponseType(ResponseType.SERVER_IS_BUSY);
        response.setDetail("can not allocate result buffer");
        response.clear();
      }

      int tryNum = 10;
      while (tryNum-- > 0) {
        try {
          buf = ByteBufUtils.newByteBuf(4 + response.bufferLen(), false);
          if (buf != null) {
            break;
          }

          Thread.sleep(5000);
        } catch (Throwable ex) {
          oom();
          LOG.error("allocate result buffer for response " + response + " failed ", ex);
        }
      }
    }
    return buf;
    */
  }

  private void oom() {
    context.getRunningContext().oom();
  }

  /**
   * Update a partition use PSF
   *
   * @param request rpc request
   * @param in      serialized rpc request
   * @return response
   */
  private UpdaterResponse update(UpdaterRequest request, ByteBuf in) {
    // Get partition and check the partition state
    PartitionKey partKey = request.getPartKey();
    ServerPartition part =
      context.getMatrixStorageManager().getPart(partKey.getMatrixId(), partKey.getPartitionId());
    if (part == null) {
      String log = "update " + request + " failed. The partition " + partKey + " does not exist";
      LOG.fatal(log);
      return new UpdaterResponse(ResponseType.SERVER_HANDLE_FATAL, log);
    }

    PartitionState state = part.getState();
    if (state != PartitionState.READ_AND_WRITE) {
      String log = "update " + request + " failed. The partition " + partKey + " state is " + state;
      LOG.error(log);
      return new UpdaterResponse(ResponseType.PARTITION_READ_ONLY, log);
    }

    // Get the stored pss for this partition
    PartitionLocation partLoc = null;
    try {
      partLoc =
        context.getMatrixMetaManager().getPartLocation(request.getPartKey(), disableRouterCache);
    } catch (Throwable x) {
      String log = "update " + request + " failed, get partition location from master failed " + x
        .getMessage();
      LOG.error(log, x);
      return new UpdaterResponse(ResponseType.SERVER_HANDLE_FAILED, log);
    }

    // Check this ps is the master ps for this location, only master ps can accept the update
    if (!request.isComeFromPs() && !isPartMasterPs(partLoc)) {
      String log =
        "update " + request + " failed, update to slave ps for partition " + request.getPartKey();
      LOG.error(log);
      return new UpdaterResponse(ResponseType.SERVER_HANDLE_FAILED, log);
    } else {
      try {
        Class<? extends UpdateFunc> funcClass =
          (Class<? extends UpdateFunc>) Class.forName(request.getUpdaterFuncClass());
        Constructor<? extends UpdateFunc> constructor = funcClass.getConstructor();
        constructor.setAccessible(true);
        UpdateFunc func = constructor.newInstance();
        func.setPsContext(context);

        // Check the partition state again
        state = part.getState();
        if (state != PartitionState.READ_AND_WRITE) {
          String log =
            "update " + request + " failed. The partition " + partKey + " state is " + state;
          LOG.error(log);
          return new UpdaterResponse(ResponseType.SERVER_HANDLE_FAILED, log);
        }

        part.update(func, request.getPartParam());
        if (partLoc.psLocs.size() > 1) {
          // Start to put the update to the slave pss
          // TODO
          // context.getPS2PSPusher().put(request, in, partLoc);
        }
        return new UpdaterResponse(ResponseType.SUCCESS);
      } catch (WaitLockTimeOutException | OutOfMemoryError e) {
        String log = "update " + request + " failed " + StringUtils.stringifyException(e);
        LOG.error(log, e);
        return new UpdaterResponse(ResponseType.SERVER_IS_BUSY, log);
      } catch (Throwable e) {
        String log = "update " + request + " failed " + StringUtils.stringifyException(e);
        LOG.fatal(log, e);
        return new UpdaterResponse(ResponseType.SERVER_HANDLE_FATAL, log);
      }
    }
  }

  /**
   * Get clocks for all matrices partition
   *
   * @param request rpc request
   * @return response contains clocks
   */
  private GetClocksResponse getClocks(GetClocksRequest request) {
    Map<PartitionKey, Integer> clocks = context.getClockVectorManager().getPartClocksFromCache();
    return new GetClocksResponse(ResponseType.SUCCESS, null, clocks);
  }

  /**
   * Get a batch of row splits
   *
   * @param request rpc request
   * @return response contains row splits
   */
  private GetRowsSplitResponse getRowsSplit(GetRowsSplitRequest request) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("get row request=" + request);
    }

    PartitionKey partKey = request.getPartKey();
    try {
      if (!isClockReady(partKey, request.getClock())) {
        return new GetRowsSplitResponse(ResponseType.CLOCK_NOTREADY, "clock not ready");
      } else {
        List<ServerRow> rows = new ArrayList<ServerRow>();
        List<Integer> rowIndexes = request.getRowIndexes();
        if (rowIndexes != null) {
          int size = rowIndexes.size();
          for (int i = 0; i < size; i++) {
            ServerRow row = context.getMatrixStorageManager()
                .getRow(partKey.getMatrixId(), rowIndexes.get(i), partKey.getPartitionId());
            if (row != null) {
              rows.add(row);
            }
          }
        }

        return new GetRowsSplitResponse(ResponseType.SUCCESS, rows);
      }
    } catch (WaitLockTimeOutException | OutOfMemoryError e) {
      String log = "get rows " + request + " failed " + StringUtils.stringifyException(e);
      LOG.error(log, e);
      return new GetRowsSplitResponse(ResponseType.SERVER_IS_BUSY, log);
    } catch (Throwable e) {
      String log = "get rows " + request + " failed " + StringUtils.stringifyException(e);
      LOG.fatal(log, e);
      return new GetRowsSplitResponse(ResponseType.SERVER_HANDLE_FATAL, log);
    }
  }

  /**
   * Get matrix partition
   *
   * @param request rpc request
   * @return response contains whole partition
   */
  private GetPartitionResponse getPartition(GetPartitionRequest request) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("get partition request=" + request);
    }
    PartitionKey partKey = request.getPartKey();

    try {
      if (!isClockReady(partKey, request.getClock())) {
        return new GetPartitionResponse(ResponseType.CLOCK_NOTREADY, "clock not ready");
      } else {
        ServerPartition partition =
            context.getMatrixStorageManager().getPart(partKey.getMatrixId(), partKey.getPartitionId());
        return new GetPartitionResponse(ResponseType.SUCCESS, partition);
      }
    } catch (WaitLockTimeOutException | OutOfMemoryError e) {
      String log = "get partition " + request + " failed " + StringUtils.stringifyException(e);
      LOG.error(log, e);
      return new GetPartitionResponse(ResponseType.SERVER_IS_BUSY, log);
    } catch (Throwable e) {
      String log = "get partition " + request + " failed " + StringUtils.stringifyException(e);
      LOG.fatal(log, e);
      return new GetPartitionResponse(ResponseType.SERVER_HANDLE_FATAL, log);
    }
  }

  /**
   * Get a row split
   *
   * @param request rpc request
   * @return response contains the row split
   */
  private GetRowSplitResponse getRowSplit(GetRowSplitRequest request) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("get row request=" + request);
    }

    PartitionKey partKey = request.getPartKey();
    try {
      GetRowSplitResponse response = new GetRowSplitResponse();
      if (!isClockReady(partKey, request.getClock())) {
        return new GetRowSplitResponse(ResponseType.CLOCK_NOTREADY, "clock not ready");
      } else {
        ServerRow row = context.getMatrixStorageManager()
            .getRow(partKey.getMatrixId(), request.getRowIndex(), partKey.getPartitionId());
        row.setClock(context.getClockVectorManager()
            .getPartClock(partKey.getMatrixId(), partKey.getPartitionId()));
        response.setResponseType(ResponseType.SUCCESS);
        response.setRowSplit(row);
        return new GetRowSplitResponse(ResponseType.SUCCESS, row);
      }
    } catch (WaitLockTimeOutException | OutOfMemoryError e) {
      String log = "get row " + request + " failed " + StringUtils.stringifyException(e);
      LOG.error(log, e);
      return new GetRowSplitResponse(ResponseType.SERVER_IS_BUSY, log);
    } catch (Throwable e) {
      String log = "get row " + request + " failed " + StringUtils.stringifyException(e);
      LOG.fatal(log, e);
      return new GetRowSplitResponse(ResponseType.SERVER_HANDLE_FATAL, log);
    }
  }

  private boolean isClockReady(PartitionKey partKey, int clock) {
    boolean ready = clock < 0 ||
      context.getClockVectorManager().getPartClock(partKey.getMatrixId(), partKey.getPartitionId())
        >= clock;
    if (!ready) {
      try {
        Int2ObjectOpenHashMap<Int2IntOpenHashMap> clocks =
          context.getMaster().getTaskMatrixClocks();
        context.getClockVectorManager().adjustClocks(clocks);
      } catch (ServiceException e) {
        LOG.error("Get Clocks from master falied,", e);
      }
      ready = clock < 0 || context.getClockVectorManager()
        .getPartClock(partKey.getMatrixId(), partKey.getPartitionId()) >= clock;
    }
    return ready;
  }

  /**
   * Update a matrix partition
   *
   * @param request rpc request
   * @param in      serialized request
   * @return response
   */
  private UpdateResponse update(UpdateRequest request, ByteBuf in) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("put update request=" + request);
    }

    // Get partition and check the partition state
    PartitionKey partKey = request.getPartKey();
    ServerPartition part =
      context.getMatrixStorageManager().getPart(partKey.getMatrixId(), partKey.getPartitionId());
    if (part == null) {
      String log = "update " + request + " failed. The partition " + partKey + " does not exist";
      LOG.fatal(log);
      return new UpdateResponse(ResponseType.SERVER_HANDLE_FATAL, log);
    }

    PartitionState state = part.getState();
    if (!request.isComeFromPs() && state != PartitionState.READ_AND_WRITE) {
      String log = "update " + request + " failed. The partition " + partKey + " state is " + state;
      LOG.error(log);
      return new UpdateResponse(ResponseType.SERVER_HANDLE_FAILED, log);
    }

    // Get the stored pss for this partition
    PartitionLocation partLoc = null;
    try {
      partLoc =
        context.getMatrixMetaManager().getPartLocation(request.getPartKey(), disableRouterCache);
    } catch (Throwable x) {
      String log = "update " + request + " failed, get partition location from master failed " + x
        .getMessage();
      LOG.error(log, x);
      return new UpdateResponse(ResponseType.SERVER_HANDLE_FAILED, log);
    }

    // Check this ps is the master ps for this partition, if not, just return failed
    if (!request.isComeFromPs() && !isPartMasterPs(partLoc)) {
      String log = "local ps is " + context.getPSAttemptId().getPsId() + " update " + request
        + " failed, update to slave ps for partition " + request.getPartKey();
      LOG.error(log);
      return new UpdateResponse(ResponseType.SERVER_HANDLE_FAILED, log);
    } else {
      int clock = request.getClock();
      partKey = request.getPartKey();
      int taskIndex = request.getTaskIndex();
      boolean updateClock = request.isUpdateClock();

      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "update split request matrixId = " + partKey.getMatrixId() + ", partId = " + partKey
            .getPartitionId() + " clock = " + clock + ", taskIndex=" + taskIndex
            + ", updateClock = " + updateClock);
      }

      try {
        state = part.getState();
        if (state != PartitionState.READ_AND_WRITE) {
          String log =
            "update " + request + " failed. The partition " + partKey + " state is " + state;
          LOG.error(log);
          return new UpdateResponse(ResponseType.SERVER_HANDLE_FAILED, log);
        }

        part.update(in, request.getOp());
        if (updateClock) {
          context.getClockVectorManager()
            .updateClock(partKey.getMatrixId(), partKey.getPartitionId(), taskIndex, clock);
        }

        // Start to put the update to the slave pss
        // TODO
        /*
        if (partLoc.psLocs.size() > 1) {
          context.getPS2PSPusher().put(request, in, partLoc);
          if (updateClock) {
            context.getPS2PSPusher().updateClock(request.getPartKey(), taskIndex, clock, partLoc);
          }
        }*/
        return new UpdateResponse(ResponseType.SUCCESS);
      } catch (WaitLockTimeOutException | OutOfMemoryError e) {
        String log = "update " + request + " failed " + StringUtils.stringifyException(e);
        LOG.error(log, e);
        return new UpdateResponse(ResponseType.SERVER_IS_BUSY, log);
      } catch (Throwable e) {
        String log = "update " + request + " failed " + StringUtils.stringifyException(e);
        LOG.fatal(log, e);
        return new UpdateResponse(ResponseType.SERVER_HANDLE_FATAL, log);
      }
    }
  }

  /**
   * Recover a partition
   *
   * @param request request
   * @return response
   */
  private Response recoverPart(RecoverPartRequest request) {
    /*if (LOG.isDebugEnabled()) {
      LOG.debug("recover part request=" + request);
    }

    long startTs = System.currentTimeMillis();
    PartitionKey partKey = request.getPartKey();
    Int2IntOpenHashMap clockVec = request.getTaskIndexToClockMap();
    if (clockVec != null) {
      context.getClockVectorManager()
        .setClockVec(partKey.getMatrixId(), partKey.getPartitionId(), clockVec);
    }

    ServerPartition part =
      context.getMatrixStorageManager().getPart(partKey.getMatrixId(), partKey.getPartitionId());
    if (part == null) {
      String log = "can not find the partition " + partKey;
      return new Response(ResponseType.SERVER_HANDLE_FATAL, log);
    }
    part.recover(request.getPart());
    if (LOG.isDebugEnabled()) {
      LOG.debug("recover partition  request " + request + " use time=" + (System.currentTimeMillis()
        - startTs));
    }*/

    return new Response(ResponseType.SUCCESS);
  }

  /**
   * Update clock value for matrix partition
   *
   * @param request request
   * @return response
   */
  private Response updateClock(UpdateClockRequest request) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("update partition clock request=" + request);
    }

    context.getClockVectorManager()
      .updateClock(request.getPartKey().getMatrixId(), request.getPartKey().getPartitionId(),
        request.getTaskIndex(), request.getClock());
    return new Response(ResponseType.SUCCESS);
  }
}
