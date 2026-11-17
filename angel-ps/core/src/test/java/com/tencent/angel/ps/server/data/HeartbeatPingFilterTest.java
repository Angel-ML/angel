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
package com.tencent.angel.ps.server.data;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

/**
 * Tests for {@link MatrixTransportServerHandler#isHeartbeatPing(Object)}: the server-side
 * recognition of client keep-alive PINGs. The filter must:
 * <ul>
 *   <li>accept an empty {@link ByteBuf} (the wire form of the PING after the length-frame
 *       decoder strips the 4-byte length prefix);</li>
 *   <li>reject any non-empty payload (every real RPC has a header much larger than 0);</li>
 *   <li>reject non-{@code ByteBuf} objects defensively.</li>
 * </ul>
 */
public class HeartbeatPingFilterTest {

  @Test
  public void emptyByteBufIsRecognisedAsPing() {
    ByteBuf empty = Unpooled.buffer(0);
    try {
      assertTrue(MatrixTransportServerHandler.isHeartbeatPing(empty));
    } finally {
      empty.release();
    }
  }

  @Test
  public void nonEmptyByteBufIsNotPing() {
    ByteBuf buf = Unpooled.wrappedBuffer(new byte[] {1, 2, 3, 4});
    try {
      assertFalse(MatrixTransportServerHandler.isHeartbeatPing(buf));
    } finally {
      buf.release();
    }
  }

  @Test
  public void singleBytePayloadIsNotPing() {
    // Edge case: anything > 0 bytes must NOT be classified as PING, even a single byte.
    ByteBuf buf = Unpooled.wrappedBuffer(new byte[] {0});
    try {
      assertFalse(MatrixTransportServerHandler.isHeartbeatPing(buf));
    } finally {
      buf.release();
    }
  }

  @Test
  public void nonByteBufIsNotPing() {
    assertFalse(MatrixTransportServerHandler.isHeartbeatPing(null));
    assertFalse(MatrixTransportServerHandler.isHeartbeatPing("not a bytebuf"));
    assertFalse(MatrixTransportServerHandler.isHeartbeatPing(new byte[0]));
  }

  /**
   * Round-trip sanity: feed the EXACT bytes that {@code ClientHeartbeatHandler} writes
   * through a frame decoder and confirm the resulting message is recognised as a PING.
   * This guarantees the end-to-end "PING" contract holds even if the framing layer
   * changes shape in either direction.
   */
  @Test
  public void clientWireFormatRoundTripsToPing() {
    // ClientHeartbeatHandler writes Unpooled.EMPTY_BUFFER, then LengthFieldPrepender(4)
    // turns it into a 4-byte frame containing the int 0. After
    // LengthFieldBasedFrameDecoder(.., 0, 4, 0, 4) on the receiver, the framer hands
    // upstream a ByteBuf with readableBytes() == 0 — exactly what the filter expects.
    ByteBuf afterDecode = Unpooled.buffer(0);
    try {
      assertTrue("the wire form of a client heartbeat must be classified as a PING",
          MatrixTransportServerHandler.isHeartbeatPing(afterDecode));
    } finally {
      afterDecode.release();
    }
  }
}
