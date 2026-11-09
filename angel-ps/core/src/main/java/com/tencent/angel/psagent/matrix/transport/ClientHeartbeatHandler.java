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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Application-level keep-alive on the PS client side. Works together with the
 * {@link io.netty.handler.timeout.IdleStateHandler} installed in
 * {@link MatrixTransportClient}: when no write has happened on the channel for the configured
 * writer-idle window, an empty (0-byte payload) frame is pushed onto the channel.
 *
 * <p><b>Why a 0-byte frame is enough.</b> The pipeline already contains
 * {@code LengthFieldPrepender(4)} on the writer side and
 * {@code LengthFieldBasedFrameDecoder(maxMessageSize, 0, 4, 0, 4)} on the reader side, so
 * writing an empty {@link ByteBuf} produces a valid 4-byte length frame with payload length 0
 * on the wire. The receiving server's framer dutifully decodes one frame with zero readable
 * bytes; {@code MatrixTransportServerHandler#channelRead} then recognises and discards it
 * without invoking the worker pool. No protocol change is needed and no new request type has
 * to be reserved.
 *
 * <p><b>Why writer-idle (not all-idle).</b> Write traffic alone is sufficient to refresh
 * conntrack / SLB idle timers, and triggering on writer-idle avoids the situation where a
 * client that's only <em>receiving</em> answers (e.g. waiting on a long-running PSF response)
 * needlessly fires a heartbeat. Read-idle is also irrelevant because, by the time a read has
 * been silent for the same window, the link is almost certainly already half-broken.
 *
 * <p><b>Why no pong is required.</b> The middlebox we are defeating only cares about
 * <em>any</em> packet flowing through the connection. A unidirectional PING is sufficient to
 * keep its idle timer reset; the server is not expected to respond.
 *
 * <p><b>Self-disable on first write failure.</b> If the heartbeat write itself fails, this
 * handler permanently disables heartbeats on the offending channel for the remainder of its
 * lifetime. Two reasons:
 * <ul>
 *   <li>It avoids a tight loop of "write fails → IdleStateEvent fires immediately again →
 *       another failed write" against a half-closed channel that the upstream pipeline is
 *       already in the process of tearing down.</li>
 *   <li>It guards against the subtle "new client + old PS server" upgrade window where the
 *       PS does not yet contain the matching 0-byte filter and would treat the empty frame
 *       as a malformed RPC and close the connection. Disabling heartbeats per-channel after
 *       the first such failure prevents the new client from itself triggering a churn loop
 *       on a partially-upgraded cluster. The freshly-reconnected channel will start with a
 *       healthy heartbeat path; only the broken one is silenced.</li>
 * </ul>
 */
public class ClientHeartbeatHandler extends ChannelDuplexHandler {

  private static final Log LOG = LogFactory.getLog(ClientHeartbeatHandler.class);

  /**
   * Singleton, immutable, refcount-1 empty payload. Each {@code writeAndFlush} below calls
   * {@code retainedDuplicate()} so the handler can be shared across channels and event loops
   * without the netty allocator ever being touched on the heartbeat path.
   */
  private static final ByteBuf EMPTY_PING = Unpooled.unreleasableBuffer(Unpooled.EMPTY_BUFFER);

  /**
   * Per-handler-instance flag (the handler is {@code @Sharable=false} — one instance per
   * pipeline — so a plain field is safe and avoids the visibility cost of volatile on the
   * write path of a non-event-loop check).
   */
  private boolean disabled = false;

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof IdleStateEvent) {
      IdleStateEvent e = (IdleStateEvent) evt;
      // Only react to WRITER_IDLE; we install IdleStateHandler with read-idle=0 and
      // all-idle=0 so this is what we will actually receive in practice. Guard anyway in
      // case the configuration is changed in the future.
      switch (e.state()) {
        case WRITER_IDLE:
          if (!disabled && ctx.channel().isActive()) {
            // Fire-and-forget: a failure here just means the link is already broken, in
            // which case channelInactive will land in the dispatcher anyway. We DO listen
            // on the future, however, so that a single failed heartbeat permanently
            // disables further heartbeats on this channel — see class javadoc.
            ctx.writeAndFlush(EMPTY_PING.retainedDuplicate())
                .addListener(future -> {
                  if (!future.isSuccess()) {
                    disabled = true;
                    LOG.error("Netty heartbeat write failed on " + ctx.channel()
                        + ", disabling heartbeats on this channel", future.cause());
                  }
                });
          }
          return;
        default:
          // Fall through to the default chain so the event is observable upstream.
      }
    }
    super.userEventTriggered(ctx, evt);
  }
}
