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

import com.tencent.angel.PartitionKey;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc;
import com.tencent.angel.ml.matrix.transport.*;
import com.tencent.angel.ps.impl.MatrixPartitionManager;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.utils.ByteBufUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The Matrix transport server handler,which offer matrix services for client.
 */
public class MatrixTransportServerHandler extends ChannelInboundHandlerAdapter {
  private static final Log LOG = LogFactory.getLog(MatrixTransportServerHandler.class);
  private final static boolean useDirectorBuffer;
  private final static ConcurrentHashMap<ChannelHandlerContext, AtomicBoolean> channelStates;
  private final static ExecutorService workerPool;
  private final static boolean isUseSender;
  private final static ExecutorService senderPool;
  public final static ConcurrentHashMap<Integer, Long> seqIdToSendTsMap = new ConcurrentHashMap<>();

  static {
    channelStates = new ConcurrentHashMap<ChannelHandlerContext, AtomicBoolean>();
    if (PSContext.get() != null && PSContext.get().getPs() != null) {
      Configuration conf = PSContext.get().getConf();
      useDirectorBuffer = conf
        .getBoolean(AngelConf.ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEDIRECTBUFFER,
          AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEDIRECTBUFFER);

      ByteBufUtils.useDirect = useDirectorBuffer;

      workerPool = Executors.newFixedThreadPool(PSContext.get().getConf()
        .getInt(AngelConf.ANGEL_MATRIXTRANSFER_SERVER_WORKER_POOL_SIZE,
          AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_WORKER_POOL_SIZE));

      isUseSender = PSContext.get().getConf()
        .getBoolean(AngelConf.ANGEL_MATRIXTRANSFER_SERVER_USER_SENDER,
          AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_USER_SENDER);

      senderPool = isUseSender ?
        Executors.newFixedThreadPool(PSContext.get().getConf()
          .getInt(AngelConf.ANGEL_MATRIXTRANSFER_SERVER_SENDER_POOL_SIZE,
            AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_SENDER_POOL_SIZE)) :
        null;
    } else {
      useDirectorBuffer = AngelConf.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEDIRECTBUFFER;
      workerPool = Executors
        .newFixedThreadPool(AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_WORKER_POOL_SIZE);
      isUseSender = AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_USER_SENDER;
      senderPool = isUseSender ?
        Executors
          .newFixedThreadPool(AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_SENDER_POOL_SIZE) :
        null;
    }
  }

  class Processor extends Thread {
    private ByteBuf message;
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


  class Sender extends Thread {
    private Object result;
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

  public MatrixTransportServerHandler() {

  }

  @Override public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
    LOG.debug("channel " + ctx.channel() + " registered");
    channelStates.put(ctx, new AtomicBoolean(false));
    super.channelRegistered(ctx);
  }

  @Override public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    LOG.debug("channel " + ctx.channel() + " unregistered");
    channelStates.remove(ctx);
    super.channelRegistered(ctx);
  }

  @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (needAsync(msg)) {
      workerPool.execute(new Processor((ByteBuf) msg, ctx));
    } else {
      handle(ctx, msg, true);
    }
  }

  private boolean needAsync(Object msg) {
    ByteBuf in = (ByteBuf) msg;
    in.readInt();
    int methodId = in.readInt();
    in.resetReaderIndex();
    TransportMethod method = TransportMethod.typeIdToTypeMap.get(methodId);
    if (method == TransportMethod.GET_CLOCKS) {
      return false;
    } else {
      return true;
    }
  }

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
          seqIdToSendTsMap.put(seqId, System.currentTimeMillis());
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

  private void sendResult(ChannelHandlerContext ctx, Object result, boolean useSync) {
    if (!useSync && isUseSender) {
      senderPool.execute(new Sender(ctx, result));
    } else {
      send(ctx, result);
    }
  }

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
          result = getRowSplit(seqId, request, ctx);
          break;
        }

        case GET_ROWSSPLIT: {
          GetRowsSplitRequest request = new GetRowsSplitRequest();
          request.deserialize(in);
          result = getRowsSplit(seqId, request, ctx);
          break;
        }

        case GET_PART: {
          GetPartitionRequest request = new GetPartitionRequest();
          request.deserialize(in);
          result = getPartition(seqId, request, ctx);
          break;
        }

        case PUT_PARTUPDATE: {
          PutPartitionUpdateRequest request = new PutPartitionUpdateRequest();
          request.deserialize(in);
          result = putPartUpdate(ctx, seqId, methodId, request, in);
          break;
        }

        case PUT_PART: {
          result = putPart(ctx, seqId, methodId, in);
          break;
        }

        case GET_CLOCKS: {
          GetClocksRequest request = new GetClocksRequest();
          request.deserialize(in);
          result = getClocks(seqId, request, ctx);
          break;
        }

        case UPDATER: {
          UpdaterRequest request = new UpdaterRequest();
          request.deserialize(in);
          result = update(seqId, request, ctx);
          break;
        }

        case GET_UDF: {
          GetUDFRequest request = new GetUDFRequest();
          request.deserialize(in);
          result = getSplit(seqId, request, ctx);
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
      in.release();
    }
  }

  @SuppressWarnings("unchecked")
  private ByteBuf getSplit(int seqId, GetUDFRequest request, ChannelHandlerContext ctx) {
    GetUDFResponse response = null;
    try {
      Class<? extends GetFunc> funcClass =
        (Class<? extends GetFunc>) Class.forName(request.getGetFuncClass());
      Constructor<? extends GetFunc> constructor = funcClass.getConstructor();
      constructor.setAccessible(true);
      GetFunc func = constructor.newInstance();
      PartitionGetResult partResult = func.partitionGet(request.getPartParam());
      response = new GetUDFResponse(partResult);
      response.setResponseType(ResponseType.SUCCESS);
    } catch (Exception e) {
      LOG.fatal("get udf request " + request + " failed ", e);
      response = new GetUDFResponse();
      response.setDetail("get udf request failed " + e.getMessage());
      response.setResponseType(ResponseType.FATAL);
    }

    ByteBuf buf = ByteBufUtils.newByteBuf(4 + response.bufferLen(), useDirectorBuffer);
    buf.writeInt(seqId);
    response.serialize(buf);
    return buf;
  }

  @SuppressWarnings("unchecked")
  private ByteBuf update(int seqId, UpdaterRequest request, ChannelHandlerContext ctx) {
    UpdaterResponse response = null;
    try {
      Class<? extends UpdateFunc> funcClass =
        (Class<? extends UpdateFunc>) Class.forName(request.getUpdaterFuncClass());
      Constructor<? extends UpdateFunc> constructor = funcClass.getConstructor();
      constructor.setAccessible(true);
      UpdateFunc func = constructor.newInstance();
      func.partitionUpdate(request.getPartParam());
      response = new UpdaterResponse();
      response.setResponseType(ResponseType.SUCCESS);
    } catch (Exception e) {
      LOG.fatal("updater request " + request + " failed ", e);
      response = new UpdaterResponse();
      response.setDetail("updater request failed " + e.getMessage());
      response.setResponseType(ResponseType.FATAL);
    }

    ByteBuf buf = ByteBufUtils.newByteBuf(4 + response.bufferLen(), useDirectorBuffer);
    buf.writeInt(seqId);
    response.serialize(buf);
    return buf;
  }

  private ByteBuf getClocks(int seqId, GetClocksRequest request, ChannelHandlerContext ctx) {
    Object2IntOpenHashMap<PartitionKey> clocks = new Object2IntOpenHashMap<PartitionKey>();
    PSContext.get().getMatrixPartitionManager().getClocks(clocks);
    GetClocksResponse response = new GetClocksResponse(ResponseType.SUCCESS, null, clocks);

    ByteBuf buf = ByteBufUtils.newByteBuf(4 + response.bufferLen(), useDirectorBuffer);
    buf.writeInt(seqId);
    response.serialize(buf);
    return buf;
  }

  private ByteBuf getRowsSplit(int seqId, GetRowsSplitRequest request, ChannelHandlerContext ctx) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("get row request=" + request);
    }
    PartitionKey partKey = request.getPartKey();
    int clock = request.getClock();

    long startTs = System.currentTimeMillis();
    GetRowsSplitResponse response = new GetRowsSplitResponse();

    if (!PSContext.get().getMatrixPartitionManager().partitionReady(partKey, clock)) {
      response.setResponseType(ResponseType.NOTREADY);
    } else {
      List<ServerRow> rows = new ArrayList<ServerRow>();
      List<Integer> rowIndexes = request.getRowIndexes();
      if (rowIndexes != null) {
        int size = rowIndexes.size();
        for (int i = 0; i < size; i++) {
          ServerRow row = PSContext.get().getMatrixPartitionManager()
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

  private ByteBuf getPartition(int seqId, GetPartitionRequest request, ChannelHandlerContext ctx) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("get partition request=" + request);
    }
    PartitionKey partKey = request.getPartKey();
    int clock = request.getClock();

    long startTs = System.currentTimeMillis();
    GetPartitionResponse response = new GetPartitionResponse();

    if (!PSContext.get().getMatrixPartitionManager().partitionReady(partKey, clock)) {
      response.setResponseType(ResponseType.NOTREADY);
    } else {
      ServerPartition partition = PSContext.get().getMatrixPartitionManager()
        .getPartition(partKey.getMatrixId(), partKey.getPartitionId());
      response.setResponseType(ResponseType.SUCCESS);
      response.setPartition(partition);
    }

    ByteBuf buf = ByteBufUtils.newByteBuf(4 + response.bufferLen(), useDirectorBuffer);
    buf.writeInt(seqId);
    response.serialize(buf);

    return buf;
  }

  private ByteBuf getRowSplit(int seqId, GetRowSplitRequest request, ChannelHandlerContext ctx) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("get row request=" + request + " with seqId=" + seqId);
    }

    PartitionKey partKey = request.getPartKey();
    int clock = request.getClock();

    long startTs = System.currentTimeMillis();
    GetRowSplitResponse response = new GetRowSplitResponse();

    if (!PSContext.get().getMatrixPartitionManager().partitionReady(partKey, clock)) {
      response.setResponseType(ResponseType.NOTREADY);
    } else {
      ServerRow row = PSContext.get().getMatrixPartitionManager()
        .getRow(partKey.getMatrixId(), request.getRowIndex(), partKey.getPartitionId());
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

  private ByteBuf putPartUpdate(ChannelHandlerContext ctx, int seqId, int methodId,
    PutPartitionUpdateRequest request, ByteBuf in) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("put update request=" + request + " with seqId=" + seqId);
    }

    long startTs = System.currentTimeMillis();
    int clock = request.getClock();
    PartitionKey partKey = request.getPartKey();
    int taskIndex = request.getTaskIndex();
    boolean updateClock = request.isUpdateClock();

    ByteBuf buf = ByteBufUtils.newByteBuf(8 + 4);
    buf.writeInt(seqId);

    if (LOG.isDebugEnabled()) {
      LOG.debug("seqId = " + seqId + " update split request matrixId = " + partKey.getMatrixId()
        + ", partId = " + partKey.getPartitionId() + " clock = " + clock + ", taskIndex="
        + taskIndex + ", updateClock = " + updateClock);
    }

    PutPartitionUpdateResponse resposne = null;
    try {
      PSContext.get().getMatrixPartitionManager().update(partKey, in);
      if (updateClock) {
        PSContext.get().getMatrixPartitionManager().clock(partKey, taskIndex, clock);
      }
      resposne = new PutPartitionUpdateResponse(ResponseType.SUCCESS);
    } catch (Exception x) {
      LOG.fatal("put update failed.", x);
      resposne = new PutPartitionUpdateResponse(ResponseType.FATAL, x.getMessage());
    }

    resposne.serialize(buf);

    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "update partition for request " + request + " use time=" + (System.currentTimeMillis()
          - startTs) + ", response buf=" + buf);
    }
    return buf;
  }

  @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    cause.printStackTrace();
    ctx.close();
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
    if (!PSContext.get().getMatrixPartitionManager().partitionReady(partKey, clock)) {
      resposne = new Response(ResponseType.NOTREADY);
      // resposne.encode(buf);
      // TODO:
    } else {
      resposne = new Response(ResponseType.SUCCESS);
      // resposne.encode(buf);
      // TODO:
      MatrixPartitionManager matPartManager = PSContext.get().getMatrixPartitionManager();

      int rslen = in.readInt();
      for (int i = 0; i < rslen - 1; i++) {
        int rowId = str + i;
        len = in.readInt();
        matPartManager.getRow(matId, rowId, partId).encode(in, buf, len);
      }
    }

    ctx.writeAndFlush(buf);
  }

  private ByteBuf putPart(ChannelHandlerContext ctx, int seqId, int methodId, ByteBuf in) {
    PartitionKey partKey = new PartitionKey();
    partKey.deserialize(in);

    ByteBuf buf = ByteBufUtils.newByteBuf(8 + 4);
    buf.writeInt(seqId);
    buf.writeInt(methodId);

    try {
      PSContext.get().getMatrixPartitionManager().update(partKey, in);
      Response resposne = new Response(ResponseType.SUCCESS);
      // resposne.encode(buf);
      // TODO:
    } catch (Exception x) {
      x.printStackTrace();
      Response resposne = new Response(ResponseType.FATAL, x.getMessage());
      // resposne.encode(buf);
      // TODO:
    }

    return buf;
  }
}
