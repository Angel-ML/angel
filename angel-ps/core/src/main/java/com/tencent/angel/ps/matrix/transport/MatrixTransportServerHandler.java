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
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.ml.matrix.transport.*;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ml.matrix.psf.updater.base.UpdaterFunc;
import com.tencent.angel.ps.impl.MatrixPartitionManager;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.utils.ByteBufUtils;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

/**
 * The Matrix transport server handler,which offer matrix services for client.
 */
public class MatrixTransportServerHandler extends ChannelInboundHandlerAdapter {
  private static final Log LOG = LogFactory.getLog(MatrixTransportServerHandler.class);
  private final boolean useDirectorBuffer;

  public MatrixTransportServerHandler() {
    Configuration conf = PSContext.get().getConf();
    useDirectorBuffer =
        conf.getBoolean(AngelConfiguration.ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEDIRECTBUFFER,
            AngelConfiguration.DEFAULT_ANGEL_NETTY_MATRIXTRANSFER_SERVER_USEDIRECTBUFFER);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) {
    LOG.info("channel active");
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    ByteBuf in = (ByteBuf) msg;
    int seqId = in.readInt();
    int methodId = in.readInt();
    TransportMethod method = TransportMethod.typeIdToTypeMap.get(methodId);

    LOG.debug("receive a request method=" + method + ", seqId=" + seqId);

    switch (method) {
      case GET_ROWSPLIT: {
        GetRowSplitRequest request = new GetRowSplitRequest();
        request.deserialize(in);
        getRowSplit(seqId, request, ctx);
        break;
      }

      case GET_ROWSSPLIT: {
        GetRowsSplitRequest request = new GetRowsSplitRequest();
        request.deserialize(in);
        getRowsSplit(seqId, request, ctx);
        break;
      }

      case GET_PART: {
        GetPartitionRequest request = new GetPartitionRequest();
        request.deserialize(in);
        getPartition(seqId, request, ctx);
        break;
      }

      case PUT_PARTUPDATE: {
        putPartUpdate(ctx, seqId, methodId, in);
        break;
      }

      case PUT_PART: {
        putPart(ctx, seqId, methodId, in);
        break;
      }

      case GET_CLOCKS: {
        GetClocksRequest request = new GetClocksRequest();
        request.deserialize(in);
        getClocks(seqId, request, ctx);
        break;
      }

      case UPDATER: {
        UpdaterRequest request = new UpdaterRequest();
        request.deserialize(in);
        update(seqId, request, ctx);
        break;
      }

      case GET_UDF: {
        GetUDFRequest request = new GetUDFRequest();
        request.deserialize(in);
        getSplit(seqId, request, ctx);
        break;
      }
      default:
        break;
    }
    in.release();
  }

  @SuppressWarnings("unchecked")
  private void getSplit(int seqId, GetUDFRequest request, ChannelHandlerContext ctx) {
    GetUDFResponse response = null;
    try {
      Class<? extends GetFunc> funcClass = (Class<? extends GetFunc>) Class.forName(request.getGetFuncClass());
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
    ctx.writeAndFlush(buf);
  }

  @SuppressWarnings("unchecked")
  private void update(int seqId, UpdaterRequest request, ChannelHandlerContext ctx) {
    UpdaterResponse response = null;
    try {
      Class<? extends UpdaterFunc> funcClass = (Class<? extends UpdaterFunc>) Class.forName(request.getUpdaterFuncClass());
      Constructor<? extends UpdaterFunc> constructor = funcClass.getConstructor();
      constructor.setAccessible(true);
      UpdaterFunc func = constructor.newInstance();
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
    ctx.writeAndFlush(buf);
  }

  private void getClocks(int seqId, GetClocksRequest request, ChannelHandlerContext ctx) {
    Object2IntOpenHashMap<PartitionKey> clocks = new Object2IntOpenHashMap<PartitionKey>();
    PSContext.get().getMatrixPartitionManager().getClocks(clocks);
    GetClocksResponse response = new GetClocksResponse(ResponseType.SUCCESS, null, clocks);

    ByteBuf buf = ByteBufUtils.newByteBuf(4 + response.bufferLen(), useDirectorBuffer);
    buf.writeInt(seqId);
    response.serialize(buf);
    ctx.writeAndFlush(buf);
  }

  private void getRowsSplit(int seqId, GetRowsSplitRequest request, ChannelHandlerContext ctx) {
    LOG.debug("get row request=" + request);
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
          ServerRow row =
              PSContext.get().getMatrixPartitionManager()
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

    long endTs = System.currentTimeMillis();
    LOG.debug("get rows request " + request + " serialze time=" + (endTs - startTs));
    ctx.writeAndFlush(buf);
    LOG.debug("get rows request " + request + " start to send response time "
        + (System.currentTimeMillis()));
  }

  private void getPartition(int seqId, GetPartitionRequest request, ChannelHandlerContext ctx) {
    LOG.debug("get partition request=" + request);
    PartitionKey partKey = request.getPartKey();
    int clock = request.getClock();

    long startTs = System.currentTimeMillis();
    GetPartitionResponse response = new GetPartitionResponse();

    if (!PSContext.get().getMatrixPartitionManager().partitionReady(partKey, clock)) {
      response.setResponseType(ResponseType.NOTREADY);
    } else {
      ServerPartition partition =
          PSContext.get().getMatrixPartitionManager()
              .getPartition(partKey.getMatrixId(), partKey.getPartitionId());
      response.setResponseType(ResponseType.SUCCESS);
      response.setPartition(partition);
    }

    ByteBuf buf = ByteBufUtils.newByteBuf(4 + response.bufferLen(), useDirectorBuffer);
    buf.writeInt(seqId);
    response.serialize(buf);

    long endTs = System.currentTimeMillis();
    LOG.debug("get partition request " + request + " serialze time=" + (endTs - startTs));
    ctx.writeAndFlush(buf);
    LOG.debug("get partition request " + request + " start to send response time "
        + (System.currentTimeMillis()));
  }

  private void getRowSplit(int seqId, GetRowSplitRequest request, ChannelHandlerContext ctx) {
    LOG.debug("get row request=" + request);
    PartitionKey partKey = request.getPartKey();
    int clock = request.getClock();

    long startTs = System.currentTimeMillis();
    GetRowSplitResponse response = new GetRowSplitResponse();

    if (!PSContext.get().getMatrixPartitionManager().partitionReady(partKey, clock)) {
      response.setResponseType(ResponseType.NOTREADY);
    } else {
      ServerRow row =
          PSContext.get().getMatrixPartitionManager()
              .getRow(partKey.getMatrixId(), request.getRowIndex(), partKey.getPartitionId());
      response.setResponseType(ResponseType.SUCCESS);
      response.setRowSplit(row);
    }

    ByteBuf buf = ByteBufUtils.newByteBuf(4 + response.bufferLen(), useDirectorBuffer);
    buf.writeInt(seqId);
    response.serialize(buf);

    long endTs = System.currentTimeMillis();
    LOG.debug("get row request " + request + " serialze time=" + (endTs - startTs));
    ctx.writeAndFlush(buf);
    LOG.debug("get row request " + request + " start to send response time "
        + (System.currentTimeMillis()));
  }

  private void putPartUpdate(ChannelHandlerContext ctx, int seqId, int methodId, ByteBuf in) {
    int clock = in.readInt();
    PartitionKey partKey = new PartitionKey();
    partKey.setMatrixId(in.readInt());
    partKey.setPartitionId(in.readInt());

    int taskIndex = in.readInt();
    boolean updateClock = in.readBoolean();

    ByteBuf buf = ByteBufUtils.newByteBuf(8 + 4);
    buf.writeInt(seqId);

    LOG.debug("seqId = " + seqId + " update split request matrixId = " + partKey.getMatrixId()
        + ", partId = " + partKey.getPartitionId() + " clock = " + clock + ", updateClock = "
        + updateClock);

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
    ctx.writeAndFlush(buf);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
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

  private void putPart(ChannelHandlerContext ctx, int seqId, int methodId, ByteBuf in) {
    int matId = in.readInt();
    int partId = in.readInt();

    PartitionKey partKey = new PartitionKey();
    partKey.setMatrixId(matId);
    partKey.setPartitionId(partId);

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

    ctx.writeAndFlush(buf);
  }
}
