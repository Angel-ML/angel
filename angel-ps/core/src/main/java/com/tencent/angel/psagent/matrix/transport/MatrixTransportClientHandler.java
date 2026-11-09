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

import com.tencent.angel.utils.StringUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.AttributeKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.LinkedBlockingQueue;


/**
 * The netty client handler for ps rpc client.
 */
public class MatrixTransportClientHandler extends ChannelInboundHandlerAdapter {
  private static final Log LOG = LogFactory.getLog(MatrixTransportClientHandler.class);
  private static final AttributeKey<String> CLOSE_REASON =
      AttributeKey.valueOf("ps.client.close.reason");


  /**
   * Transport client
   */
  private final MatrixTransportClient client;

  /**
   * rpc dispatch event queue
   */
  private final LinkedBlockingQueue<DispatcherEvent> dispatchMessageQueue;

  /**
   * RPC context
   */
  private final RPCContext rpcContext;

  public MatrixTransportClientHandler(MatrixTransportClient client,
    LinkedBlockingQueue<DispatcherEvent> dispatchMessageQueue, RPCContext rpcContext) {
    this.client = client;
    this.dispatchMessageQueue = dispatchMessageQueue;
    this.rpcContext = rpcContext;
  }

  @Override public void channelActive(ChannelHandlerContext ctx) {
  }

  @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    String reason = ctx.channel().attr(CLOSE_REASON).get();
    if (reason == null) {
      reason = "peer-or-network-close";
    }
    notifyChannelClosed(ctx.channel(), reason);
    super.channelInactive(ctx);
  }

  private void notifyChannelClosed(Channel ch, String reason) throws InterruptedException {
    dispatchMessageQueue.put(new ChannelClosedEvent(ch, reason));
  }


  @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
    if (LOG.isDebugEnabled()) {
      int seqId = ((ByteBuf) msg).readInt();
      LOG.debug("receive result of seqId=" + seqId);
      ((ByteBuf) msg).resetReaderIndex();
    }

    client.handleResponse(new SerializedMsgWithTime((ByteBuf) msg));
  }

  @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable x) {
    LOG.warn("exception happened on " + ctx.channel(), x);
    String errorMsg = StringUtils.stringifyException(x);
    if (x instanceof OutOfMemoryError || (errorMsg.contains("MemoryError"))) {
      rpcContext.oom();
    } else {
      ctx.channel().attr(CLOSE_REASON)
          .set("client-exception-close:" + x.getClass().getSimpleName());
      ctx.close();
    }
  }

}
