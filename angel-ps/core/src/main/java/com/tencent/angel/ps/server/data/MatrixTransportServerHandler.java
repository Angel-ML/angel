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

import com.tencent.angel.ps.PSContext;
import com.tencent.angel.utils.StringUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * The Matrix transport server handler,which offer matrix services for client.
 */
public class MatrixTransportServerHandler extends ChannelInboundHandlerAdapter {
  private static final Log LOG = LogFactory.getLog(MatrixTransportServerHandler.class);
  private static final AttributeKey<String> CLOSE_REASON = AttributeKey.valueOf("ps.server.close.reason");
  private static final AtomicBoolean FIRST_ACCEPT_LOGGED = new AtomicBoolean(false);

  private final PSContext context;
  private final WorkerPool workerPool;

  public MatrixTransportServerHandler(PSContext context) {
    this.context = context;
    this.workerPool = context.getWorkerPool();
  }

  @Override public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
    LOG.debug("channel " + ctx.channel() + " registered");
    super.channelRegistered(ctx);
  }

  @Override public void channelActive(ChannelHandlerContext ctx) throws Exception {
    if (FIRST_ACCEPT_LOGGED.compareAndSet(false, true)) {
      LOG.info("[PS-SERVER-READY][FIRST-ACCEPT] channel=" + ctx.channel()
          + ", local=" + ctx.channel().localAddress()
          + ", remote=" + ctx.channel().remoteAddress());
    }
    super.channelActive(ctx);
  }

  @Override public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
    LOG.debug("channel " + ctx.channel() + " unregistered");
    super.channelUnregistered(ctx);
  }


  @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
    // Drop client-side keep-alive PINGs (see isHeartbeatPing javadoc).
    if (isHeartbeatPing(msg)) {
      LOG.debug("Received Netty heartbeat (0-byte frame) from " + ctx.channel() + ", dropping it");
      ReferenceCountUtil.release(msg);
      return;
    }
    workerPool.handlerRequest(ctx, msg);
  }

  /**
   * True iff {@code msg} is a client-side keep-alive PING produced by
   * {@code com.tencent.angel.psagent.matrix.transport.ClientHeartbeatHandler}: a valid
   * 4-byte length-prepended frame with payload length 0. Kept package-private and static
   * so that it is unit-testable without standing up a real {@code WorkerPool}.
   *
   * <p>The PING's sole purpose is to push a packet through any conntrack / SLB / firewall
   * in the path so the long-lived TCP session is not silently reaped while idle. No real
   * RPC ever serializes to a zero-byte body — every {@code RequestHeader} alone occupies
   * many bytes — so this filter is unambiguous.
   */
  static boolean isHeartbeatPing(Object msg) {
    return msg instanceof ByteBuf && ((ByteBuf) msg).readableBytes() == 0;
  }

  @Override public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    String reason = ctx.channel().attr(CLOSE_REASON).get();
    if (reason == null) {
      reason = "peer-or-network-close";
    }
    LOG.warn("[PS-SERVER-CHANNEL][INACTIVE] channel=" + ctx.channel()
        + ", reason=" + reason
        + ", local=" + ctx.channel().localAddress()
        + ", remote=" + ctx.channel().remoteAddress());
    super.channelInactive(ctx);
  }

  /**
   * Triggered by the {@link io.netty.handler.timeout.IdleStateHandler} installed in {@link
   * MatrixTransportServer}. When a channel has been completely idle (no read and no write)
   * for longer than the configured window we proactively close it. The client receives
   * {@code channelInactive}, invalidates the corresponding entry in its connection pool, and
   * a subsequent request triggers a fresh {@code bootstrap.connect()} that produces a brand
   * new TCP session — one that cannot have been silently reaped by an intermediate NAT /
   * firewall while sitting idle on the long-lived pool.
   *
   * Closing an idle channel is intentionally preferred over sending an application-level
   * heartbeat: it requires no protocol change and the client side already routes
   * {@code channelInactive} into its {@code CHANNEL_CLOSED} event flow which reschedules any
   * in-flight requests onto a different channel.
   */
  @Override public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof IdleStateEvent) {
      String reason = "server-idle-timeout:" + ((IdleStateEvent) evt).state();
      ctx.channel().attr(CLOSE_REASON).set(reason);
      LOG.info("[PS-SERVER-CHANNEL][ACTIVE-CLOSE] channel=" + ctx.channel()
          + ", reason=" + reason
          + ", local=" + ctx.channel().localAddress()
          + ", remote=" + ctx.channel().remoteAddress());
      ctx.close();
      return;
    }
    super.userEventTriggered(ctx, evt);
  }

  @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    String errorMsg = StringUtils.stringifyException(cause);
    if (cause instanceof OutOfMemoryError || (errorMsg.contains("MemoryError"))) {
      context.getRunningContext().oom();
    }
    String reason = "server-exception-close:" + cause.getClass().getSimpleName();
    ctx.channel().attr(CLOSE_REASON).set(reason);
    LOG.error("[PS-SERVER-CHANNEL][ACTIVE-CLOSE] channel=" + ctx.channel()
        + ", reason=" + reason
        + ", local=" + ctx.channel().localAddress()
        + ", remote=" + ctx.channel().remoteAddress(), cause);
    ctx.close();
  }
}
