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
import com.tencent.angel.ml.matrix.PartitionLocation;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc;
import com.tencent.angel.ml.matrix.transport.*;
import com.tencent.angel.ps.impl.MatrixStorageManager;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.*;
import com.tencent.angel.utils.ByteBufUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
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

/**
 * RPC worker pool for matrix transformation
 */
public class WorkerPool {
  private static final Log LOG = LogFactory.getLog(WorkerPool.class);
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

  /**
   * Create a WorkerPool
   * @param context PS context
   */
  public WorkerPool(PSContext context) {
    this.context = context;
    channelStates = new ConcurrentHashMap<ChannelHandlerContext, AtomicBoolean>();
    Configuration conf = context.getConf();
    useDirectorBuffer = conf.getBoolean(
      AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEDIRECTBUFFER,
      AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEDIRECTBUFFER);

    ByteBufUtils.useDirect = useDirectorBuffer;
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
    if (needAsync(msg)) {
      workerPool.execute(new Processor((ByteBuf) msg, ctx));
    } else {
      handle(ctx, msg, true);
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
     * Response
     */
    private Object result;

    /**
     * Channel context
     */
    private ChannelHandlerContext ctx;

    Sender(ChannelHandlerContext ctx, Object result) {
      this.result = result;
      this.ctx = ctx;
    }

    @Override public void run() {
      send(ctx, result);
      result = null;
      ctx = null;
    }
  }

  /**
   * Check the request is a simple request: has short processing time
   * @param msg request
   * @return true means it's a simple request
   */
  private boolean needAsync(Object msg) {
    ByteBuf in = (ByteBuf) msg;
    in.readInt();
    int methodId = in.readInt();
    in.resetReaderIndex();
    TransportMethod method = TransportMethod.typeIdToTypeMap.get(methodId);
    if (method == TransportMethod.GET_CLOCKS || method == TransportMethod.UPDATE_CLOCK) {
      return false;
    } else {
      return true;
    }
  }

  /**
   * Send back the result
   * @param ctx channel context
   * @param result rpc result
   */
  private void send(ChannelHandlerContext ctx, Object result) {
    int seqId = 0;
    Channel ch = ctx.channel();

    try {
      seqId = ((ByteBuf) result).readInt();
      ((ByteBuf) result).resetReaderIndex();
      AtomicBoolean channelInUse = channelStates.get(ctx);
      if (channelInUse == null) {
        return;
      }
      long startTs = System.currentTimeMillis();
      while (true) {
        if (channelInUse.compareAndSet(false, true)) {
          ctx.writeAndFlush(result);
          channelInUse.set(false);
          LOG.debug(
            "send response buf=" + result + ",channel ctx=" + ctx.channel() + ", seqId=" + seqId
              + " use time=" + (System.currentTimeMillis() - startTs));
          return;
        }
        Thread.sleep(10);
      }
    } catch (Throwable ex) {
      LOG.error("send response of request failed, request seqId=" + seqId + ", channel=" + ch, ex);
    }
  }

  /**
   * Send the rpc result
   * @param ctx channel context
   * @param result rpc result
   * @param useSync true means send it directly, false means send it use sender
   */
  private void sendResult(ChannelHandlerContext ctx, Object result, boolean useSync) {
    if (!useSync && useSender) {
      senderPool.execute(new Sender(ctx, result));
    } else {
      send(ctx, result);
    }
  }

  /**
   * Handle the request
   * @param ctx channel context
   * @param msg rpc request
   * @param useSync true means handle it directly, false means handle it use Processor
   */
  private void handle(ChannelHandlerContext ctx, Object msg, boolean useSync) {
    int seqId = 0;
    int methodId = 0;
    TransportMethod method = TransportMethod.GET_ROWSPLIT;
    ByteBuf in = (ByteBuf) msg;

    try {
      seqId = in.readInt();
      methodId = in.readInt();
      method = TransportMethod.typeIdToTypeMap.get(methodId);
      ByteBuf result = null;
      switch (method) {
        case GET_ROWSPLIT: {
          GetRowSplitRequest request = new GetRowSplitRequest();
          request.deserialize(in);
          result = getRowSplit(seqId, request);
          break;
        }

        case GET_ROWSSPLIT: {
          GetRowsSplitRequest request = new GetRowsSplitRequest();
          request.deserialize(in);
          result = getRowsSplit(seqId, request);
          break;
        }

        case GET_PART: {
          GetPartitionRequest request = new GetPartitionRequest();
          request.deserialize(in);
          result = getPartition(seqId, request);
          break;
        }

        case PUT_PARTUPDATE: {
          PutPartitionUpdateRequest request = new PutPartitionUpdateRequest();
          request.deserialize(in);
          result = putPartUpdate(seqId,request, in);
          break;
        }

        case PUT_PART: {
          result = putPart(seqId, methodId, in);
          break;
        }

        case GET_CLOCKS: {
          GetClocksRequest request = new GetClocksRequest();
          request.deserialize(in);
          result = getClocks(seqId, request);
          break;
        }

        case UPDATER: {
          UpdaterRequest request = new UpdaterRequest();
          request.deserialize(in);
          result = update(seqId, request, in);
          break;
        }

        case GET_UDF: {
          GetUDFRequest request = new GetUDFRequest();
          request.deserialize(in);
          result = getSplit(seqId, request);
          break;
        }

        case RECOVER_PART: {
          RecoverPartRequest request = new RecoverPartRequest();
          request.deserialize(in);
          result = recoverPart(seqId, request);
          break;
        }

        case UPDATE_CLOCK: {
          UpdateClockRequest request = new UpdateClockRequest();
          request.deserialize(in);
          result = updateClock(seqId, request);
          break;
        }

        default:
          break;
      }
      sendResult(ctx, result, useSync);
    } catch (Throwable ex) {
      LOG.error(
        "handler response of request failed, request seqId=" + seqId + ", request method=" + method,
        ex);
    } finally {
      if(in.refCnt() > 0) {
        in.release();
      }
    }
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
   * @param seqId rpc request id
   * @param request request
   * @return serialized rpc response contain the get result
   */
  private ByteBuf getSplit(int seqId, GetUDFRequest request) {
    GetUDFResponse response = null;
    try {
      Class<? extends GetFunc> funcClass =
        (Class<? extends GetFunc>) Class.forName(request.getGetFuncClass());
      Constructor<? extends GetFunc> constructor = funcClass.getConstructor();
      constructor.setAccessible(true);
      GetFunc func = constructor.newInstance();
      func.setPsContext(context);
      PartitionGetResult partResult = func.partitionGet(request.getPartParam());
      response = new GetUDFResponse(partResult);
      response.setResponseType(ResponseType.SUCCESS);
    } catch (Throwable e) {
      LOG.fatal("get udf request " + request + " failed ", e);
      response = new GetUDFResponse();
      response.setDetail("get udf request failed " + e.getMessage());
      response.setResponseType(ResponseType.SERVER_HANDLE_FATAL);
    }

    ByteBuf buf = ByteBufUtils.newByteBuf(4 + response.bufferLen(), useDirectorBuffer);
    buf.writeInt(seqId);
    response.serialize(buf);
    return buf;
  }

  /**
   * Update a partition use PSF
   * @param seqId rpc request id
   * @param request rpc request
   * @param in serialized rpc request
   * @return serialized rpc response
   */
  private ByteBuf update(int seqId, UpdaterRequest request, ByteBuf in) {
    UpdaterResponse response = null;
    ByteBuf buf = ByteBufUtils.newByteBuf(4 + 8, useDirectorBuffer);

    // Get partition and check the partition state
    PartitionKey partKey = request.getPartKey();
    ServerPartition part = context.getMatrixStorageManager().getPart(partKey.getMatrixId(), partKey.getPartitionId());
    if(part == null) {
      String log = "update " + request + " failed. The partition " + partKey + " does not exist";
      LOG.fatal(log);
      response = new UpdaterResponse(ResponseType.SERVER_HANDLE_FATAL, log);
      response.serialize(buf);
      return buf;
    }

    PartitionState state = part.getState();
    if(state != PartitionState.READ_AND_WRITE) {
      String log = "update " + request + " failed. The partition " + partKey + " state is " + state;
      LOG.error(log);
      response = new UpdaterResponse(ResponseType.PARTITION_READ_ONLY, log);
      response.serialize(buf);
      return buf;
    }

    // Get the stored pss for this partition
    PartitionLocation partLoc = null;
    try {
      partLoc =
        context.getMatrixMetaManager().getPartLocation(request.getPartKey(), disableRouterCache);
    } catch (Throwable x) {
      String log = "update " + request + " failed, get partition location from master failed " + x.getMessage();
      LOG.error(log, x);
      response = new UpdaterResponse(ResponseType.SERVER_HANDLE_FAILED, log);
      response.serialize(buf);
      return buf;
    }

    // Check this ps is the master ps for this location, only master ps can accept the update
    if(!request.isComeFromPs() && !isPartMasterPs(partLoc)) {
      String log = "update " + request + " failed, update to slave ps for partition " + request.getPartKey();
      LOG.error(log);
      response = new UpdaterResponse(ResponseType.SERVER_HANDLE_FAILED, log);
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
          response = new UpdaterResponse(ResponseType.SERVER_HANDLE_FAILED, log);
          response.serialize(buf);
          return buf;
        }

        part.update(func, request.getPartParam());
        response = new UpdaterResponse();
        response.setResponseType(ResponseType.SUCCESS);

        if(partLoc.psLocs.size() > 1) {
          // Start to put the update to the slave pss
          context.getPS2PSPusher().put(request, in, partLoc);
        }
      } catch (Throwable e) {
        String log = "update " + request + " failed " + e.getMessage();
        LOG.fatal(log, e);
        response = new UpdaterResponse(ResponseType.SERVER_HANDLE_FATAL, log);
      }
    }

    buf.writeInt(seqId);
    response.serialize(buf);
    return buf;
  }

  /**
   * Get clocks for all matrices partition
   * @param seqId rpc request id
   * @param request rpc request
   * @return serialized rpc response contains clocks
   */
  private ByteBuf getClocks(int seqId, GetClocksRequest request) {
    Map<PartitionKey, Integer> clocks = context.getClockVectorManager().getPartClocksFromCache();
    GetClocksResponse response = new GetClocksResponse(ResponseType.SUCCESS, null, clocks);

    ByteBuf buf = ByteBufUtils.newByteBuf(4 + response.bufferLen(), useDirectorBuffer);
    buf.writeInt(seqId);
    response.serialize(buf);
    return buf;
  }

  /**
   * Get a batch of row splits
   * @param seqId rpc request id
   * @param request rpc request
   * @return serialized rpc response contains row splits
   */
  private ByteBuf getRowsSplit(int seqId, GetRowsSplitRequest request) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("get row request=" + request);
    }
    PartitionKey partKey = request.getPartKey();
    int clock = request.getClock();

    GetRowsSplitResponse response = new GetRowsSplitResponse();
    if (!isClockReady(partKey, clock)) {
      response.setResponseType(ResponseType.CLOCK_NOTREADY);
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

      response.setResponseType(ResponseType.SUCCESS);
      response.setRowsSplit(rows);
    }

    ByteBuf buf = ByteBufUtils.newByteBuf(4 + response.bufferLen(), useDirectorBuffer);
    buf.writeInt(seqId);
    response.serialize(buf);

    return buf;
  }

  /**
   * Get matrix partition
   * @param seqId rpc request id
   * @param request rpc request
   * @return serialized rpc response contains whole partition
   */
  private ByteBuf getPartition(int seqId, GetPartitionRequest request) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("get partition request=" + request);
    }
    PartitionKey partKey = request.getPartKey();
    int clock = request.getClock();

    long startTs = System.currentTimeMillis();
    GetPartitionResponse response = new GetPartitionResponse();

    if (!isClockReady(partKey, clock)) {
      response.setResponseType(ResponseType.CLOCK_NOTREADY);
    } else {
      ServerPartition partition = context.getMatrixStorageManager()
        .getPart(partKey.getMatrixId(), partKey.getPartitionId());
      response.setResponseType(ResponseType.SUCCESS);
      response.setPartition(partition);
    }

    ByteBuf buf = ByteBufUtils.newByteBuf(4 + response.bufferLen(), useDirectorBuffer);
    buf.writeInt(seqId);
    response.serialize(buf);

    return buf;
  }

  /**
   * Get a row split
   * @param seqId rpc request id
   * @param request rpc request
   * @return serialized rpc response contains the row split
   */
  private ByteBuf getRowSplit(int seqId, GetRowSplitRequest request) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("get row request=" + request + " with seqId=" + seqId);
    }

    PartitionKey partKey = request.getPartKey();
    int clock = request.getClock();

    long startTs = System.currentTimeMillis();
    GetRowSplitResponse response = new GetRowSplitResponse();

    if (!isClockReady(partKey, clock)) {
      response.setResponseType(ResponseType.CLOCK_NOTREADY);
    } else {
      ServerRow row = context.getMatrixStorageManager()
        .getRow(partKey.getMatrixId(), request.getRowIndex(), partKey.getPartitionId());
      row.setClock(context.getClockVectorManager().getPartClock(partKey.getMatrixId(), partKey.getPartitionId()));
      response.setResponseType(ResponseType.SUCCESS);
      response.setRowSplit(row);
    }

    ByteBuf buf = ByteBufUtils.newByteBuf(4 + response.bufferLen(), useDirectorBuffer);
    buf.writeInt(seqId);
    response.serialize(buf);
    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "response for request " + request + " serialize use time=" + (System.currentTimeMillis()
          - startTs) + ", response buf=" + buf);
    }
    return buf;
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
   * @param seqId rpc request id
   * @param request rpc request
   * @param in serialized request
   * @return serialized rpc response
   */
  private ByteBuf putPartUpdate(int seqId, PutPartitionUpdateRequest request, ByteBuf in) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("put update request=" + request + " with seqId=" + seqId);
    }
    long startTs = System.currentTimeMillis();
    ByteBuf buf = ByteBufUtils.newByteBuf(8 + 4);
    buf.writeInt(seqId);
    PutPartitionUpdateResponse response = null;

    // Get partition and check the partition state
    PartitionKey partKey = request.getPartKey();
    ServerPartition part = context.getMatrixStorageManager().getPart(partKey.getMatrixId(), partKey.getPartitionId());
    if(part == null) {
      String log = "update " + request + " failed. The partition " + partKey + " does not exist";
      LOG.fatal(log);
      response = new PutPartitionUpdateResponse(ResponseType.SERVER_HANDLE_FATAL, log);
      response.serialize(buf);
      return buf;
    }

    PartitionState state = part.getState();
    if(!request.isComeFromPs() && state != PartitionState.READ_AND_WRITE) {
      String log = "update " + request + " failed. The partition " + partKey + " state is " + state;
      LOG.error(log);
      response = new PutPartitionUpdateResponse(ResponseType.SERVER_HANDLE_FAILED, log);
      response.serialize(buf);
      return buf;
    }

    // Get the stored pss for this partition
    PartitionLocation partLoc = null;
    try {
      partLoc =
        context.getMatrixMetaManager().getPartLocation(request.getPartKey(), disableRouterCache);
    } catch (Throwable x) {
      String log = "update " + request + " failed, get partition location from master failed " + x.getMessage();
      LOG.error(log, x);
      response = new PutPartitionUpdateResponse(ResponseType.SERVER_HANDLE_FAILED, log);
      response.serialize(buf);
      return buf;
    }

    // Check this ps is the master ps for this partition, if not, just return failed
    if(!request.isComeFromPs() && !isPartMasterPs(partLoc)) {
      String log = "local ps is " + context.getPSAttemptId().getPsId() +  " update "
        + request + " failed, update to slave ps for partition " + request.getPartKey();
      LOG.error(log);
      response = new PutPartitionUpdateResponse(ResponseType.SERVER_HANDLE_FAILED, log);
    }  else {
      int clock = request.getClock();
      partKey = request.getPartKey();
      int taskIndex = request.getTaskIndex();
      boolean updateClock = request.isUpdateClock();

      if (LOG.isDebugEnabled()) {
        LOG.debug("seqId = " + seqId + " update split request matrixId = " + partKey.getMatrixId()
          + ", partId = " + partKey.getPartitionId() + " clock = " + clock + ", taskIndex="
          + taskIndex + ", updateClock = " + updateClock);
      }

      try {
        state = part.getState();
        if(state != PartitionState.READ_AND_WRITE) {
          String log = "update " + request + " failed. The partition " + partKey + " state is " + state;
          LOG.error(log);
          response = new PutPartitionUpdateResponse(ResponseType.SERVER_HANDLE_FAILED, log);
          response.serialize(buf);
          return buf;
        }

        part.update(in, rowUpdater);
        if (updateClock) {
          context.getClockVectorManager().updateClock(partKey.getMatrixId(), partKey.getPartitionId(), taskIndex, clock);
        }
        response = new PutPartitionUpdateResponse(ResponseType.SUCCESS);

        // Start to put the update to the slave pss
        if(partLoc.psLocs.size() > 1) {
          context.getPS2PSPusher().put(request, in, partLoc);
          if(updateClock) {
            context.getPS2PSPusher().updateClock(request.getPartKey(), taskIndex, clock, partLoc);
          }
        }

      } catch (Throwable x) {
        String log = "update " + request + " failed " + x.getMessage();
        LOG.fatal(log, x);
        response = new PutPartitionUpdateResponse(ResponseType.SERVER_HANDLE_FATAL, log);
      }
    }

    response.serialize(buf);
    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "update partition for request " + request + " use time=" + (System.currentTimeMillis()
          - startTs) + ", response buf=" + buf);
    }
    return buf;
  }

  @SuppressWarnings("unused")
  private void getPart(ChannelHandlerContext ctx, int seqId, int methodId, ByteBuf in) {
    int partId = in.readInt();
    int matId = in.readInt();
    int clock = in.readInt();
    int str = in.readInt();

    PartitionKey partKey = new PartitionKey();
    partKey.setMatrixId(matId);
    partKey.setPartitionId(partId);

    int len = in.readInt();
    ByteBuf buf = ByteBufUtils.newByteBuf(8 + len * 4, useDirectorBuffer);
    buf.writeInt(seqId);
    buf.writeInt(methodId);

    Response resposne = null;
    if (!isClockReady(partKey, clock)) {
      resposne = new Response(ResponseType.CLOCK_NOTREADY);
      // resposne.encode(buf);
      // TODO:
    } else {
      resposne = new Response(ResponseType.SUCCESS);
      // resposne.encode(buf);
      // TODO:
      MatrixStorageManager matPartManager = context.getMatrixStorageManager();

      int rslen = in.readInt();
      for (int i = 0; i < rslen - 1; i++) {
        int rowId = str + i;
        len = in.readInt();
        matPartManager.getRow(matId, rowId, partId).encode(in, buf, len);
      }
    }

    ctx.writeAndFlush(buf);
  }

  private ByteBuf putPart(int seqId, int methodId, ByteBuf in) {
    PartitionKey partKey = new PartitionKey();
    partKey.deserialize(in);

    ByteBuf buf = ByteBufUtils.newByteBuf(8 + 4);
    buf.writeInt(seqId);
    buf.writeInt(methodId);

    try {
      //context.getMatrixStorageManager().update(partKey, in);
      Response resposne = new Response(ResponseType.SUCCESS);
      // resposne.encode(buf);
      // TODO:
    } catch (Exception x) {
      x.printStackTrace();
      Response resposne = new Response(ResponseType.SERVER_HANDLE_FATAL, x.getMessage());
      // resposne.encode(buf);
      // TODO:
    }

    return buf;
  }

  /**
   * Recover a partition
   * @param seqId rpc request it
   * @param request request
   * @return serialized rpc response
   */
  private ByteBuf recoverPart(int seqId, RecoverPartRequest request) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("recover part request=" + request + " with seqId=" + seqId);
    }

    long startTs = System.currentTimeMillis();
    ByteBuf buf = ByteBufUtils.newByteBuf(8 + 4);
    buf.writeInt(seqId);

    Response response = null;

    PartitionKey partKey = request.getPartKey();
    Int2IntOpenHashMap clockVec = request.getTaskIndexToClockMap();
    if(clockVec != null) {
      context.getClockVectorManager().setClockVec(partKey.getMatrixId(), partKey.getPartitionId(), clockVec);
    }

    ServerPartition part = context.getMatrixStorageManager().getPart(partKey.getMatrixId(), partKey.getPartitionId());
    if(part == null) {
      String log = "can not find the partition " + partKey;
      response = new Response(ResponseType.SERVER_HANDLE_FATAL, log);
      response.serialize(buf);
      return buf;
    }
    part.recover(request.getPart());
    response = new Response(ResponseType.SUCCESS);
    response.serialize(buf);
    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "recover partition  request " + request + " use time=" + (System.currentTimeMillis()
          - startTs));
    }

    return buf;
  }

  /**
   * Update clock value for matrix partition
   * @param seqId rpc request it
   * @param request request
   * @return serialized rpc response
   */
  private ByteBuf updateClock(int seqId, UpdateClockRequest request) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("update partition clock request=" + request + " with seqId=" + seqId);
    }

    long startTs = System.currentTimeMillis();
    ByteBuf buf = ByteBufUtils.newByteBuf(8 + 4);
    buf.writeInt(seqId);

    Response response;

    context.getClockVectorManager().updateClock(request.getPartKey().getMatrixId(),
      request.getPartKey().getPartitionId(), request.getTaskIndex(), request.getClock());
    response = new Response(ResponseType.SUCCESS);
    response.serialize(buf);
    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "recover partition  request " + request + " use time=" + (System.currentTimeMillis()
          - startTs));
    }

    return buf;
  }
}
