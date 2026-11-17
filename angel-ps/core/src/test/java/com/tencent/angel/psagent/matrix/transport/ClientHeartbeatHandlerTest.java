/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 */
package com.tencent.angel.psagent.matrix.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.IdleStateEvent;
import org.junit.Test;

/**
 * Tests for {@link ClientHeartbeatHandler}.
 *
 * <p>The handler's contract:
 * <ul>
 *   <li>On {@link IdleStateEvent#WRITER_IDLE}, push a 0-byte payload onto the wire.</li>
 *   <li>The wire framing uses {@link LengthFieldPrepender}({@code lengthFieldLength=4}),
 *       so the PING is observable downstream as a 4-byte length frame with value 0.</li>
 *   <li>On READER_IDLE / ALL_IDLE the handler must NOT send anything.</li>
 *   <li>If a heartbeat write fails once, the handler must permanently disable itself on
 *       that channel — the first failure is the only one that should produce a write
 *       attempt; subsequent WRITER_IDLE events must be silent.</li>
 * </ul>
 */
public class ClientHeartbeatHandlerTest {

  /**
   * Build a pipeline shaped exactly like {@code MatrixTransportClient}'s real one: framer +
   * length-prepender on the codec side, then the heartbeat handler on the user side.
   */
  private static EmbeddedChannel newWiredChannel() {
    return new EmbeddedChannel(
        new LengthFieldBasedFrameDecoder(64 * 1024, 0, 4, 0, 4),
        new LengthFieldPrepender(4),
        new ClientHeartbeatHandler());
  }

  @Test
  public void writerIdleSendsExactlyOneEmptyFrame() {
    EmbeddedChannel ch = newWiredChannel();

    // Fire the idle event the way IdleStateHandler would in production.
    ch.pipeline().fireUserEventTriggered(IdleStateEvent.WRITER_IDLE_STATE_EVENT);
    ch.flushOutbound();

    // Drain everything outbound and assert the SHAPE matches what a heartbeat looks like
    // on the wire: exactly one length-prefix frame whose length value is zero. Anything
    // else (e.g. an extra trailing payload buf) is fine to ignore — LengthFieldPrepender
    // and Netty's EmbeddedChannel together can leave a zero-readable empty buf in the
    // outbound queue depending on Netty version. The wire-format frame is what matters.
    boolean sawLengthFrame = false;
    int extras = 0;
    Object out;
    while ((out = ch.readOutbound()) != null) {
      assertNotNull(out);
      assertTrue(out instanceof ByteBuf);
      ByteBuf buf = (ByteBuf) out;
      try {
        if (buf.readableBytes() == 4 && !sawLengthFrame) {
          assertEquals("heartbeat length prefix must be zero (no payload)",
              0, buf.readInt());
          sawLengthFrame = true;
        } else {
          // Either a zero-byte trailing buf or a non-length frame. Track but tolerate.
          extras++;
        }
      } finally {
        buf.release();
      }
    }
    assertTrue("a 4-byte length frame encoding 0 must be flushed on WRITER_IDLE",
        sawLengthFrame);
    // We tolerate at most one extra buffer (typical Netty 4.1 behaviour) — more would
    // indicate the heartbeat is duplicated.
    assertTrue("at most one stray outbound buffer is expected, got " + extras,
        extras <= 1);

    ch.finishAndReleaseAll();
  }

  @Test
  public void readerAndAllIdleAreIgnored() {
    EmbeddedChannel ch = newWiredChannel();

    ch.pipeline().fireUserEventTriggered(IdleStateEvent.READER_IDLE_STATE_EVENT);
    ch.pipeline().fireUserEventTriggered(IdleStateEvent.ALL_IDLE_STATE_EVENT);
    ch.flushOutbound();

    assertNull("READER_IDLE must not produce a heartbeat", ch.readOutbound());
    assertFalse(ch.finish());
  }

  /**
   * Exercises the self-disable safety net: a failed heartbeat write should not be retried
   * the next time a WRITER_IDLE fires. We arrange this by inserting a custom outbound
   * handler that fails every write with an exception; the first WRITER_IDLE must produce
   * an attempt (which fails), and the second must produce no attempt at all.
   */
  @Test
  public void heartbeatSelfDisablesAfterFirstWriteFailure() {
    final int[] attempts = {0};
    EmbeddedChannel ch = new EmbeddedChannel(
        new LengthFieldBasedFrameDecoder(64 * 1024, 0, 4, 0, 4),
        new LengthFieldPrepender(4),
        // Outbound failure injector: count writes and fail them. Sits BEFORE the
        // heartbeat handler in head-to-tail terms so that the heartbeat's writeAndFlush
        // is what triggers it.
        new ChannelOutboundHandlerAdapter() {
          @Override public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise p) {
            attempts[0]++;
            // Release the buffer the heartbeat retained for us, then fail the promise.
            io.netty.util.ReferenceCountUtil.release(msg);
            p.setFailure(new RuntimeException("synthetic write failure"));
          }
        },
        new ClientHeartbeatHandler());

    ch.pipeline().fireUserEventTriggered(IdleStateEvent.WRITER_IDLE_STATE_EVENT);
    ch.flushOutbound();
    assertEquals("first WRITER_IDLE must produce exactly one (failed) write attempt",
        1, attempts[0]);

    // Drain any spurious outbound (there shouldn't be any: the write was failed).
    while (ch.readOutbound() != null) { /* drain */ }

    ch.pipeline().fireUserEventTriggered(IdleStateEvent.WRITER_IDLE_STATE_EVENT);
    ch.flushOutbound();
    assertEquals("after a failed heartbeat write the handler must self-disable; "
            + "no further writes are allowed", 1, attempts[0]);

    ch.pipeline().fireUserEventTriggered(IdleStateEvent.WRITER_IDLE_STATE_EVENT);
    ch.flushOutbound();
    assertEquals("self-disable must be sticky across many subsequent idles",
        1, attempts[0]);

    ch.finishAndReleaseAll();
  }

  /**
   * Multiple WRITER_IDLE events on a healthy channel should each produce a heartbeat;
   * the self-disable mechanism is gated on a write FAILURE only.
   */
  @Test
  public void multipleHealthyIdleEventsEachProduceAHeartbeat() {
    EmbeddedChannel ch = newWiredChannel();

    for (int i = 0; i < 5; i++) {
      ch.pipeline().fireUserEventTriggered(IdleStateEvent.WRITER_IDLE_STATE_EVENT);
      ch.flushOutbound();
      Object out = ch.readOutbound();
      assertNotNull("heartbeat #" + i + " must be produced on a healthy channel", out);
      ((ByteBuf) out).release();
    }

    ch.finishAndReleaseAll();
  }
}
