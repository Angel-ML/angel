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

import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.utils.StringUtils;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The Matrix transport server handler,which offer matrix services for client.
 */
public class MatrixTransportServerHandler extends ChannelInboundHandlerAdapter {
  private static final Log LOG = LogFactory.getLog(MatrixTransportServerHandler.class);
  private final PSContext context;
  private final WorkerPool workerPool;

  public MatrixTransportServerHandler(PSContext context) {
    this.context = context;
    this.workerPool = context.getWorkerPool();
  }

  @Override public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
    LOG.debug("channel " + ctx.channel() + " registered");
    workerPool.registerChannel(ctx);
    super.channelRegistered(ctx);
  }

  @Override public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    LOG.debug("channel " + ctx.channel() + " unregistered");
    workerPool.unregisterChannel(ctx);
    super.channelRegistered(ctx);
  }

  @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
    workerPool.handlerRequest(ctx, msg);
  }

  @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.error("catch a exception ", cause);
    String errorMsg = StringUtils.stringifyException(cause);
    if(cause instanceof OutOfMemoryError || (errorMsg.contains("MemoryError"))) {
      context.getRunningContext().oom();
    }
    ctx.close();
  }
}
