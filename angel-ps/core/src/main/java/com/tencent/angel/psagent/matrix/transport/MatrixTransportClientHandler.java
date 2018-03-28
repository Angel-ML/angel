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

import com.tencent.angel.utils.StringUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * The netty client handler for ps rpc client.
 */
public class MatrixTransportClientHandler extends ChannelInboundHandlerAdapter {
  private static final Log LOG = LogFactory.getLog(MatrixTransportClientHandler.class);
  /**rpc response queue*/
  private final LinkedBlockingQueue<ByteBuf> msgQueue;
  
  /**rpc dispatch event queue*/
  private final LinkedBlockingQueue<DispatcherEvent> dispatchMessageQueue;

  private final RPCContext rpcContext;

  /**
   * Create a new MatrixTransportClientHandler.
   *
   * @param msgQueue rpc response queue
   * @param dispatchMessageQueue rpc dispatch event queue
   */
  public MatrixTransportClientHandler(LinkedBlockingQueue<ByteBuf> msgQueue,
      LinkedBlockingQueue<DispatcherEvent> dispatchMessageQueue, RPCContext rpcContext) {
    this.msgQueue = msgQueue;
    this.dispatchMessageQueue = dispatchMessageQueue;
    this.rpcContext = rpcContext;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) {}

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    LOG.debug("channel " + ctx.channel() + " inactive");
    notifyChannelClosed(ctx.channel());
  }

  private void notifyChannelClosed(Channel ch) throws InterruptedException {
    dispatchMessageQueue.put(new ChannelClosedEvent(ch));
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    //LOG.debug("receive a message " + ((ByteBuf) msg).readableBytes());
    if(LOG.isDebugEnabled()) {
      int seqId = ((ByteBuf) msg).readInt();
      LOG.debug("receive result of seqId=" + seqId);
      ((ByteBuf) msg).resetReaderIndex();
    }

    try {
      msgQueue.put((ByteBuf) msg);
    } catch (InterruptedException e) {
      LOG.error("put response message queue failed ", e);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable x) {
    LOG.info("exceptin happened ", x);
    String errorMsg = StringUtils.stringifyException(x);
    if(x instanceof OutOfMemoryError || (errorMsg.contains("MemoryError"))) {
      rpcContext.oom();
    } else {
      ctx.close();
    }
  }
}
