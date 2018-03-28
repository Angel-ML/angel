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

package com.tencent.angel.ps.matrix.transport;

import com.google.protobuf.ServiceException;
import com.tencent.angel.PartitionKey;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.WaitLockTimeOutException;
import com.tencent.angel.ml.matrix.PartitionLocation;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc;
import com.tencent.angel.ml.matrix.transport.*;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.RunningContext;
import com.tencent.angel.ps.impl.matrix.*;
import com.tencent.angel.utils.ByteBufUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
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

  private final boolean usePool;

  /**
   * Use independent send workers
   */
  private final boolean useSender;

  /**
   * Get router from local cache or Master, false means get from Master
   */
  private final boolean disableRouterCache;

  /** Matrix row updater */
  private volatile RowUpdater rowUpdater;

  /**
   * RPC worker pool
   */
  private volatile ExecutorService workerPool;

  /**
   * Response sender pool
   */
  private volatile ExecutorService senderPool;

  private final RunningContext runningContext;

  /**
   * Create a WorkerPool
   * @param context PS context
   */
  public WorkerPool(PSContext context, RunningContext runningContext) {
    this.context = context;
    this.runningContext = runningContext;
    channelStates = new ConcurrentHashMap<>();
    Configuration conf = context.getConf();
    useDirectorBuffer = conf.getBoolean(
      AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEDIRECTBUFFER,
      AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEDIRECTBUFFER);

    usePool = conf.getBoolean(
      AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEPOOL,
      AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEPOOL);

    ByteBufUtils.useDirect = useDirectorBuffer;
    ByteBufUtils.usePool = usePool;
    useSender = conf.getBoolean(
      AngelConf.ANGEL_MATRIXTRANSFER_SERVER_USER_SENDER,
      AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_USER_SENDER);

    int partReplicNum = conf.getInt(AngelConf.ANGEL_PS_HA_REPLICATION_NUMBER,
      AngelConf.DEFAULT_ANGEL_PS_HA_REPLICATION_NUMBER);
    if(partReplicNum > 1) {
      disableRouterCache = true;
    } else {
      disableRouterCache = false;
    }
  }

  /**
   * Init
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public void init() throws IllegalAccessException, InstantiationException {
    Class<?> rowUpdaterClass =
      context.getConf().getClass(AngelConf.ANGEL_PS_ROW_UPDATER_CLASS,
        AngelConf.DEFAULT_ANGEL_PS_ROW_UPDATER);
    rowUpdater = (RowUpdater) rowUpdaterClass.newInstance();
  }

  /**
   * Start
   */
  public void start() {
    senderPool = useSender ?
      Executors.newFixedThreadPool(context.getConf()
        .getInt(AngelConf.ANGEL_MATRIXTRANSFER_SERVER_SENDER_POOL_SIZE,
          AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_SENDER_POOL_SIZE)) :
      null;

    workerPool = Executors.newFixedThreadPool(context.getConf().getInt(
      AngelConf.ANGEL_MATRIXTRANSFER_SERVER_WORKER_POOL_SIZE,
      AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_WORKER_POOL_SIZE));
  }

  /**
   * Stop
   */
  public void stop() {
    if(senderPool != null) {
      senderPool.shutdownNow();
    }

    if(workerPool != null) {
      workerPool.shutdownNow();
    }
  }

  /**
   * Register a Netty channel
   * @param channel Netty channel
   */
  public void registerChannel(ChannelHandlerContext channel) {
    channelStates.put(channel, new AtomicBoolean(false));
  }

  /**
   * Unregister a Netty channel
   * @param channel Netty channel
   */
  public void unregisterChannel(ChannelHandlerContext channel) {
    channelStates.remove(channel);
  }

  /**
   * Handler RPC request
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

    if(isDataRequest(method)) {
      total.incrementAndGet();
      runningContext.before(clientId, seqId);
      runningContext.relaseToken(clientId, token);
    }

    in.resetReaderIndex();
    if (needAsync(method)) {
      workerPool.execute(new Processor((ByteBuf) msg, ctx));
    } else {
      handle(ctx, msg, true);
    }
  }

  private boolean isDataRequest(TransportMethod method) {
    switch (method) {
      case GET_ROWSPLIT:
      case GET_ROWSSPLIT:
      case GET_UDF:
      case GET_PART:
      case PUT_PART:
      case PUT_PARTUPDATE:
      case UPDATER:
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

    Sender(int clientId, int seqId, TransportMethod method, ChannelHandlerContext ctx, Object result) {
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
   * @param method request type
   * @return true means it's a complex request, use async mode
   */
  private boolean needAsync(TransportMethod method) {
    return !(method == TransportMethod.GET_CLOCKS || method == TransportMethod.UPDATE_CLOCK);
  }

  /**
   * Send back the result
   * @param clientId PSClient id
   * @param seqId RPC seq id
   * @param ctx channel context
   * @param result rpc result
   */
  private void send(int clientId, int seqId, TransportMethod method, ChannelHandlerContext ctx, Object result) {
    Channel ch = ctx.channel();
    try {
      AtomicBoolean channelInUse = channelStates.get(ctx);
      if (channelInUse == null) {
        LOG.error("send response of request " + requestToString(clientId, seqId)  + ", but channel is unregistered");
        if(isDataRequest(method)) {
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
              if(isDataRequest(method)) {
                if(future.isSuccess()) {
                  normal.incrementAndGet();
                } else {
                  LOG.error("send response of request " + requestToString(clientId, seqId) + " failed ");
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
      LOG.error("send response of request failed, request seqId=" + seqId + ", channel=" + ch, ex);
      unknown.incrementAndGet();
    }
  }

  private String requestToString(int clientId, int seqId) {
    return "clientId=" + clientId + "/seqId=" + seqId;
  }

  /**
   * Send the rpc result
   * @param ctx channel context
   * @param result rpc result
   * @param useSync true means send it directly, false means send it use sender
   */
  private void sendResult(int clientId, int seqId, TransportMethod method, ChannelHandlerContext ctx, Object result, boolean useSync) {
    if (!useSync && useSender) {
      senderPool.execute(new Sender(clientId, seqId, method, ctx, result));
    } else {
      send(clientId, seqId, method, ctx, result);
    }
  }

  /**
   * Handle the request
   * @param ctx channel context
   * @param msg rpc request
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

    try {
      response = handleRPC(clientId, seqId, in, method);
    } catch (Throwable ex) {
      LOG.error("handler rpc failed ", ex);
    } finally {
      if(in.refCnt() > 0) {
        in.release();
      }
    }
    if(response == null) {
      oom.incrementAndGet();
      runningContext.after(clientId, seqId);
      return;
    }

    ByteBuf serializedResponse = null;
    try {
      serializedResponse = serializeResponse(seqId, response);
    } catch (Throwable ex) {
      oom();
      LOG.error("serialize response falied ", ex);
    }
    if(serializedResponse == null) {
      oom.incrementAndGet();
      runningContext.after(clientId, seqId);
      return;
    }
    sendResult(clientId, seqId, method, ctx, serializedResponse, useSync);
  }

  private Response handleRPC(int clientId, int seqId, ByteBuf in, TransportMethod method) throws Throwable {
    Response result;
    ServerState state = runningContext.getState();
    String log = "server is busy now, retry later";
    switch (method) {
      case GET_ROWSPLIT: {
        if(state == ServerState.BUSY) {
          result = new GetRowSplitResponse(ResponseType.SERVER_IS_BUSY, log);
        } else {
          GetRowSplitRequest request = new GetRowSplitRequest();
          request.deserialize(in);
          result = getRowSplit(request);
        }
        break;
      }

      case GET_ROWSSPLIT: {
        if(state == ServerState.BUSY) {
          result = new GetRowsSplitResponse(ResponseType.SERVER_IS_BUSY, log);
        } else {
          GetRowsSplitRequest request = new GetRowsSplitRequest();
          request.deserialize(in);
          result = getRowsSplit(request);
        }
        break;
      }

      case GET_PART: {
        if(state == ServerState.BUSY) {
          result = new GetPartitionResponse(ResponseType.SERVER_IS_BUSY, log);
        } else {
          GetPartitionRequest request = new GetPartitionRequest();
          request.deserialize(in);
          result = getPartition(request);
        }
        break;
      }

      case PUT_PARTUPDATE: {
        if(state == ServerState.BUSY) {
          result = new PutPartitionUpdateResponse(ResponseType.SERVER_IS_BUSY, log);
        } else {
          PutPartitionUpdateRequest request = new PutPartitionUpdateRequest();
          request.deserialize(in);
          result = putPartUpdate(request, in);
        }
        break;
      }

      case GET_CLOCKS: {
        GetClocksRequest request = new GetClocksRequest();
        request.deserialize(in);
        result = getClocks(request);
        break;
      }

      case UPDATER: {
        if(state == ServerState.BUSY) {
          result = new UpdaterResponse(ResponseType.SERVER_IS_BUSY, log);
        } else {
          UpdaterRequest request = new UpdaterRequest();
          request.deserialize(in);
          result = update(request, in);
        }
        break;
      }

      case GET_UDF: {
        if(state == ServerState.BUSY) {
          result = new GetUDFResponse(ResponseType.SERVER_IS_BUSY, log);
        } else {
          GetUDFRequest request = new GetUDFRequest();
          request.deserialize(in);
          result = getSplit(request);
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

      default:
        throw new UnsupportedOperationException("Unknown RPC type " + method);
    }

    if(state == ServerState.BUSY) {
      LOG.info("Hanle request " + requestToString(clientId, seqId) + " Server is BUSY now ");
      runningContext.printToken();
    }
    result.setState(state);
    return result;
  }

  private ByteBuf serializeResponse(int seqId, Response response) {
    ByteBuf buf = allocResultBuf(response);
    if(buf == null) {
      context.getPs().failed("Can not allocate any buffer now, just exit!!");
      return null;
    }

    try {
      buf.writeInt(seqId);
      response.serialize(buf);
    } catch (Throwable x) {
      response.setResponseType(ResponseType.SERVER_IS_BUSY);
      response.setDetail("can not serialize the response");
      response.clear();
      buf.release();
      buf = allocResultBuf(response);
      if(buf == null) {
        context.getPs().failed("Can not allocate any buffer now, just exit!!");
        return null;
      } else {
        buf.writeInt(seqId);
        response.serialize(buf);
      }
    }
    return buf;
  }

  /**
   * Is the PS the master ps for a partition
   * @param partLoc the stored pss for the location
   * @return true mean the PS is the master ps
   */
  private boolean isPartMasterPs(PartitionLocation partLoc) {
    return !partLoc.psLocs.isEmpty() && partLoc.psLocs.get(0).psId.equals(context.getPSAttemptId().getPsId());
  }

  /**
   * Get from the partition use PSF
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
    } catch (Throwable e) {
      LOG.fatal("get udf request " + request + " failed ", e);
      return new GetUDFResponse(ResponseType.SERVER_HANDLE_FATAL, "get udf request failed " + e.getMessage());
    }
  }

  private ByteBuf allocResultBuf(Response response) {
    ByteBuf buf = null;
    try {
      buf = ByteBufUtils.newByteBuf(4 + response.bufferLen(), useDirectorBuffer);
    } catch (Throwable x) {
      oom();
      LOG.error("allocate result buffer for response " + response + " failed ", x);
      if(response.getResponseType() == ResponseType.SUCCESS) {
        response.setResponseType(ResponseType.SERVER_IS_BUSY);
        response.setDetail("can not allocate result buffer");
        response.clear();
      }

      int tryNum = 10;
      while(tryNum-- > 0) {
        try {
          buf = ByteBufUtils.newByteBuf(4 + response.bufferLen(), false);
          if(buf != null) {
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
  }

  private void oom() {
    context.getRunningContext().oom();
  }

  /**
   * Update a partition use PSF
   * @param request rpc request
   * @param in serialized rpc request
   * @return response
   */
  private UpdaterResponse update(UpdaterRequest request, ByteBuf in) {
    // Get partition and check the partition state
    PartitionKey partKey = request.getPartKey();
    ServerPartition part = context.getMatrixStorageManager().getPart(partKey.getMatrixId(), partKey.getPartitionId());
    if(part == null) {
      String log = "update " + request + " failed. The partition " + partKey + " does not exist";
      LOG.fatal(log);
      return new UpdaterResponse(ResponseType.SERVER_HANDLE_FATAL, log);
    }

    PartitionState state = part.getState();
    if(state != PartitionState.READ_AND_WRITE) {
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
      String log = "update " + request + " failed, get partition location from master failed " + x.getMessage();
      LOG.error(log, x);
      return new UpdaterResponse(ResponseType.SERVER_HANDLE_FAILED, log);
    }

    // Check this ps is the master ps for this location, only master ps can accept the update
    if(!request.isComeFromPs() && !isPartMasterPs(partLoc)) {
      String log = "update " + request + " failed, update to slave ps for partition " + request.getPartKey();
      LOG.error(log);
      return new UpdaterResponse(ResponseType.SERVER_HANDLE_FAILED, log);
    } else {
      try {
        Class<? extends UpdateFunc> funcClass = (Class<? extends UpdateFunc>) Class.forName(request.getUpdaterFuncClass());
        Constructor<? extends UpdateFunc> constructor = funcClass.getConstructor();
        constructor.setAccessible(true);
        UpdateFunc func = constructor.newInstance();
        func.setPsContext(context);

        // Check the partition state again
        state = part.getState();
        if(state != PartitionState.READ_AND_WRITE) {
          String log = "update " + request + " failed. The partition " + partKey + " state is " + state;
          LOG.error(log);
          return new UpdaterResponse(ResponseType.SERVER_HANDLE_FAILED, log);
        }

        part.update(func, request.getPartParam());
        if(partLoc.psLocs.size() > 1) {
          // Start to put the update to the slave pss
          context.getPS2PSPusher().put(request, in, partLoc);
        }
        return new UpdaterResponse(ResponseType.SUCCESS);
      } catch (WaitLockTimeOutException wx) {
        LOG.error("wait lock timeout ", wx);
        return new UpdaterResponse(ResponseType.SERVER_IS_BUSY);
      } catch (Throwable e) {
        String log = "update " + request + " failed " + e.getMessage();
        LOG.fatal(log, e);
        return new UpdaterResponse(ResponseType.SERVER_HANDLE_FATAL, log);
      }
    }
  }

  /**
   * Get clocks for all matrices partition
   * @param request rpc request
   * @return response contains clocks
   */
  private GetClocksResponse getClocks(GetClocksRequest request) {
    Map<PartitionKey, Integer> clocks = context.getClockVectorManager().getPartClocksFromCache();
    return new GetClocksResponse(ResponseType.SUCCESS, null, clocks);
  }

  /**
   * Get a batch of row splits
   * @param request rpc request
   * @return response contains row splits
   */
  private GetRowsSplitResponse getRowsSplit(GetRowsSplitRequest request) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("get row request=" + request);
    }

    PartitionKey partKey = request.getPartKey();
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
  }

  /**
   * Get matrix partition
   * @param request rpc request
   * @return response contains whole partition
   */
  private GetPartitionResponse getPartition(GetPartitionRequest request) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("get partition request=" + request);
    }
    PartitionKey partKey = request.getPartKey();
    if (!isClockReady(partKey, request.getClock())) {
      return new GetPartitionResponse(ResponseType.CLOCK_NOTREADY, "clock not ready");
    } else {
      ServerPartition partition = context.getMatrixStorageManager()
        .getPart(partKey.getMatrixId(), partKey.getPartitionId());
      return new GetPartitionResponse(ResponseType.SUCCESS, partition);
    }
  }

  /**
   * Get a row split
   * @param request rpc request
   * @return response contains the row split
   */
  private GetRowSplitResponse getRowSplit(GetRowSplitRequest request) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("get row request=" + request);
    }

    PartitionKey partKey = request.getPartKey();
    GetRowSplitResponse response = new GetRowSplitResponse();
    if (!isClockReady(partKey, request.getClock())) {
      return new GetRowSplitResponse(ResponseType.CLOCK_NOTREADY, "clock not ready");
    } else {
      ServerRow row = context.getMatrixStorageManager()
        .getRow(partKey.getMatrixId(), request.getRowIndex(), partKey.getPartitionId());
      row.setClock(context.getClockVectorManager().getPartClock(partKey.getMatrixId(), partKey.getPartitionId()));
      response.setResponseType(ResponseType.SUCCESS);
      response.setRowSplit(row);
      return new GetRowSplitResponse(ResponseType.SUCCESS, row);
    }
  }

  private boolean isClockReady(PartitionKey partKey, int clock) {
    boolean ready = clock < 0 || context.getClockVectorManager().getPartClock(partKey.getMatrixId(), partKey.getPartitionId()) >= clock;
    if(!ready) {
      try {
        Int2ObjectOpenHashMap<Int2IntOpenHashMap> clocks = context.getMaster().getTaskMatrixClocks();
        context.getClockVectorManager().adjustClocks(clocks);
      } catch (ServiceException e) {
        LOG.error("Get Clocks from master falied,", e);
      }
      ready = clock < 0 || context.getClockVectorManager().getPartClock(partKey.getMatrixId(), partKey.getPartitionId()) >= clock;
    }
    return ready;
  }

  /**
   * Update a matrix partition
   * @param request rpc request
   * @param in serialized request
   * @return response
   */
  private PutPartitionUpdateResponse putPartUpdate(PutPartitionUpdateRequest request, ByteBuf in) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("put update request=" + request);
    }

    // Get partition and check the partition state
    PartitionKey partKey = request.getPartKey();
    ServerPartition part = context.getMatrixStorageManager().getPart(partKey.getMatrixId(), partKey.getPartitionId());
    if(part == null) {
      String log = "update " + request + " failed. The partition " + partKey + " does not exist";
      LOG.fatal(log);
      return new PutPartitionUpdateResponse(ResponseType.SERVER_HANDLE_FATAL, log);
    }

    PartitionState state = part.getState();
    if(!request.isComeFromPs() && state != PartitionState.READ_AND_WRITE) {
      String log = "update " + request + " failed. The partition " + partKey + " state is " + state;
      LOG.error(log);
      return new PutPartitionUpdateResponse(ResponseType.SERVER_HANDLE_FAILED, log);
    }

    // Get the stored pss for this partition
    PartitionLocation partLoc = null;
    try {
      partLoc =
        context.getMatrixMetaManager().getPartLocation(request.getPartKey(), disableRouterCache);
    } catch (Throwable x) {
      String log = "update " + request + " failed, get partition location from master failed " + x.getMessage();
      LOG.error(log, x);
      return new PutPartitionUpdateResponse(ResponseType.SERVER_HANDLE_FAILED, log);
    }

    // Check this ps is the master ps for this partition, if not, just return failed
    if(!request.isComeFromPs() && !isPartMasterPs(partLoc)) {
      String log = "local ps is " + context.getPSAttemptId().getPsId() +  " update "
        + request + " failed, update to slave ps for partition " + request.getPartKey();
      LOG.error(log);
      return new PutPartitionUpdateResponse(ResponseType.SERVER_HANDLE_FAILED, log);
    }  else {
      int clock = request.getClock();
      partKey = request.getPartKey();
      int taskIndex = request.getTaskIndex();
      boolean updateClock = request.isUpdateClock();

      if (LOG.isDebugEnabled()) {
        LOG.debug("update split request matrixId = " + partKey.getMatrixId()
          + ", partId = " + partKey.getPartitionId() + " clock = " + clock + ", taskIndex="
          + taskIndex + ", updateClock = " + updateClock);
      }

      try {
        state = part.getState();
        if(state != PartitionState.READ_AND_WRITE) {
          String log = "update " + request + " failed. The partition " + partKey + " state is " + state;
          LOG.error(log);
          return new PutPartitionUpdateResponse(ResponseType.SERVER_HANDLE_FAILED, log);
        }

        part.update(in, rowUpdater);
        if (updateClock) {
          context.getClockVectorManager().updateClock(partKey.getMatrixId(), partKey.getPartitionId(), taskIndex, clock);
        }

        // Start to put the update to the slave pss
        if(partLoc.psLocs.size() > 1) {
          context.getPS2PSPusher().put(request, in, partLoc);
          if(updateClock) {
            context.getPS2PSPusher().updateClock(request.getPartKey(), taskIndex, clock, partLoc);
          }
        }
        return new PutPartitionUpdateResponse(ResponseType.SUCCESS);
      } catch (WaitLockTimeOutException wx) {
        LOG.error("wait lock timeout ", wx);
        return new PutPartitionUpdateResponse(ResponseType.SERVER_IS_BUSY);
      } catch (Throwable x) {
        String log = "update " + request + " failed " + x.getMessage();
        LOG.fatal(log, x);
        return new PutPartitionUpdateResponse(ResponseType.SERVER_HANDLE_FATAL, log);
      }
    }
  }

  /**
   * Recover a partition
   * @param request request
   * @return response
   */
  private Response recoverPart(RecoverPartRequest request) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("recover part request=" + request);
    }

    long startTs = System.currentTimeMillis();
    PartitionKey partKey = request.getPartKey();
    Int2IntOpenHashMap clockVec = request.getTaskIndexToClockMap();
    if(clockVec != null) {
      context.getClockVectorManager().setClockVec(partKey.getMatrixId(), partKey.getPartitionId(), clockVec);
    }

    ServerPartition part = context.getMatrixStorageManager().getPart(partKey.getMatrixId(), partKey.getPartitionId());
    if(part == null) {
      String log = "can not find the partition " + partKey;
      return new Response(ResponseType.SERVER_HANDLE_FATAL, log);
    }
    part.recover(request.getPart());
    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "recover partition  request " + request + " use time=" + (System.currentTimeMillis()
          - startTs));
    }

    return new Response(ResponseType.SUCCESS);
  }

  /**
   * Update clock value for matrix partition
   * @param request request
   * @return response
   */
  private Response updateClock(UpdateClockRequest request) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("update partition clock request=" + request);
    }

    context.getClockVectorManager().updateClock(request.getPartKey().getMatrixId(),
      request.getPartKey().getPartitionId(), request.getTaskIndex(), request.getClock());
    return new Response(ResponseType.SUCCESS);
  }
}
