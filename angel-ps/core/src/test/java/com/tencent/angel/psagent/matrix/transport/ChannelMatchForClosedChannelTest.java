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
import static org.junit.Assert.assertTrue;

import com.tencent.angel.common.location.Location;
import com.tencent.angel.common.transport.NettyChannel;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.server.data.PSLocation;
import com.tencent.angel.ps.server.data.request.Request;
import com.tencent.angel.ps.server.data.request.RequestContext;
import io.netty.channel.Channel;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link MatrixTransportClient#matchSeqIdsForClosedChannel(Map, Map, Channel)}.
 *
 * <p><b>Bug under test.</b> Production observation: PS process dies after the client wrote
 * a request, the OS RST closes the netty channel, and the dispatcher's
 * {@code removeRequestForChannel} ought to fail every in-flight request that rode that
 * channel. The legacy implementation matched ONLY by {@code request.context.channel}, but
 * by that point the post-write {@code returnChannel(request)} had already cleared
 * {@code request.context.channel = null}. So the close-handler matched zero entries, the
 * request stayed in {@code seqIdToRequestMap} forever, and the user-visible
 * {@code FutureResult.get()} blocked indefinitely. The fix unions the legacy match with a
 * second pass over {@code seqIdToSendCFMap} whose channel-future identity outlives
 * returnChannel.
 *
 * <p>The contract under test:
 * <ul>
 *   <li>requests with non-null {@code context.channel == channel} surface in
 *       {@code viaContextChannel};</li>
 *   <li>requests with the post-write window (context.channel cleared, but cf.channel ==
 *       channel) surface in {@code viaSendCF};</li>
 *   <li>a request matching both maps is reported only once (via the legacy pass);</li>
 *   <li>requests on UNRELATED channels are not flagged;</li>
 *   <li>null channel / empty maps are no-ops.</li>
 * </ul>
 */
public class ChannelMatchForClosedChannelTest {

  private EmbeddedChannel chA;
  private EmbeddedChannel chB;

  @Before
  public void setUp() {
    chA = new EmbeddedChannel();
    chB = new EmbeddedChannel();
  }

  @After
  public void tearDown() {
    if (chA != null) chA.finishAndReleaseAll();
    if (chB != null) chB.finishAndReleaseAll();
  }

  private static Request requestWithContextChannel(NettyChannel nc) {
    Request r = new Request();
    RequestContext ctx = new RequestContext();
    ctx.setChannel(nc);
    r.setContext(ctx);
    return r;
  }

  private static Request requestWithoutContextChannel() {
    Request r = new Request();
    r.setContext(new RequestContext());
    return r;
  }

  private static MatrixTransportClient.SendResultKey sendKeyOn(Channel ch) {
    Location loc = new Location("dummy", 1);
    ParameterServerId psId = new ParameterServerId(0);
    PSLocation psLoc = new PSLocation(psId, loc);
    return new MatrixTransportClient.SendResultKey(psLoc, ch.newSucceededFuture());
  }

  // ----- core path -----

  @Test
  public void matchesEntriesViaContextChannel() {
    NettyChannel nc = new NettyChannel(chA, new Location("h", 1));

    Map<Integer, Request> seqIdToRequest = new HashMap<>();
    seqIdToRequest.put(11, requestWithContextChannel(nc));

    Map<Integer, MatrixTransportClient.SendResultKey> seqIdToSendCF = new HashMap<>();

    MatrixTransportClient.ChannelMatchResult res =
        MatrixTransportClient.matchSeqIdsForClosedChannel(seqIdToRequest, seqIdToSendCF, chA);

    assertEquals(java.util.Collections.singletonList(11), res.viaContextChannel);
    assertTrue(res.viaSendCF.isEmpty());
  }

  /** The whole point of the fix: post-write request leaks pre-fix. */
  @Test
  public void matchesEntriesViaSendCFEvenWhenContextChannelCleared() {
    Map<Integer, Request> seqIdToRequest = new HashMap<>();
    // Request lives in the map but its context.channel was already cleared by
    // returnChannel — exactly the leaking shape that hung Spark tasks for >1h.
    seqIdToRequest.put(42, requestWithoutContextChannel());

    Map<Integer, MatrixTransportClient.SendResultKey> seqIdToSendCF = new HashMap<>();
    seqIdToSendCF.put(42, sendKeyOn(chA));

    MatrixTransportClient.ChannelMatchResult res =
        MatrixTransportClient.matchSeqIdsForClosedChannel(seqIdToRequest, seqIdToSendCF, chA);

    assertTrue("legacy match must be empty when context.channel was cleared",
        res.viaContextChannel.isEmpty());
    assertEquals("the fix must catch this via the seqIdToSendCFMap pass",
        java.util.Collections.singletonList(42), res.viaSendCF);
  }

  /** Both maps point to the SAME channel for the SAME seqId — must report only once. */
  @Test
  public void doesNotDoubleFailWhenBothMapsMatch() {
    NettyChannel nc = new NettyChannel(chA, new Location("h", 1));

    Map<Integer, Request> seqIdToRequest = new HashMap<>();
    seqIdToRequest.put(7, requestWithContextChannel(nc));

    Map<Integer, MatrixTransportClient.SendResultKey> seqIdToSendCF = new HashMap<>();
    seqIdToSendCF.put(7, sendKeyOn(chA));

    MatrixTransportClient.ChannelMatchResult res =
        MatrixTransportClient.matchSeqIdsForClosedChannel(seqIdToRequest, seqIdToSendCF, chA);

    assertEquals(java.util.Collections.singletonList(7), res.viaContextChannel);
    assertTrue("seqId already claimed via context-channel pass must NOT reappear",
        res.viaSendCF.isEmpty());
  }

  /** Requests on unrelated channels must be left alone. */
  @Test
  public void leavesRequestsOnOtherChannelsAlone() {
    NettyChannel ncA = new NettyChannel(chA, new Location("a", 1));
    NettyChannel ncB = new NettyChannel(chB, new Location("b", 2));

    Map<Integer, Request> seqIdToRequest = new HashMap<>();
    seqIdToRequest.put(1, requestWithContextChannel(ncA)); // matches chA
    seqIdToRequest.put(2, requestWithContextChannel(ncB)); // unrelated
    seqIdToRequest.put(3, requestWithoutContextChannel()); // matches chA via cf

    Map<Integer, MatrixTransportClient.SendResultKey> seqIdToSendCF = new HashMap<>();
    seqIdToSendCF.put(2, sendKeyOn(chB));                  // unrelated
    seqIdToSendCF.put(3, sendKeyOn(chA));                  // post-write window
    seqIdToSendCF.put(99, sendKeyOn(chB));                 // unrelated

    MatrixTransportClient.ChannelMatchResult res =
        MatrixTransportClient.matchSeqIdsForClosedChannel(seqIdToRequest, seqIdToSendCF, chA);

    Collection<Integer> all = new HashSet<>();
    all.addAll(res.viaContextChannel);
    all.addAll(res.viaSendCF);
    assertEquals("only seqIds 1 (via ctx) and 3 (via cf) belong to chA",
        new HashSet<>(Arrays.asList(1, 3)), all);
  }

  // ----- guards -----

  @Test
  public void emptyMapsAreNoop() {
    MatrixTransportClient.ChannelMatchResult res =
        MatrixTransportClient.matchSeqIdsForClosedChannel(
            new HashMap<Integer, Request>(),
            new HashMap<Integer, MatrixTransportClient.SendResultKey>(),
            chA);
    assertTrue(res.viaContextChannel.isEmpty());
    assertTrue(res.viaSendCF.isEmpty());
  }

  @Test
  public void nullChannelIsNoop() {
    NettyChannel nc = new NettyChannel(chA, new Location("h", 1));
    Map<Integer, Request> seqIdToRequest = new HashMap<>();
    seqIdToRequest.put(1, requestWithContextChannel(nc));
    Map<Integer, MatrixTransportClient.SendResultKey> seqIdToSendCF = new HashMap<>();
    seqIdToSendCF.put(1, sendKeyOn(chA));

    MatrixTransportClient.ChannelMatchResult res =
        MatrixTransportClient.matchSeqIdsForClosedChannel(seqIdToRequest, seqIdToSendCF, null);

    assertTrue(res.viaContextChannel.isEmpty());
    assertTrue(res.viaSendCF.isEmpty());
  }
}
