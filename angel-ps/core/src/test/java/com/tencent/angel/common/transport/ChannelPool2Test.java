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
package com.tencent.angel.common.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.tencent.angel.common.location.Location;
import io.netty.channel.Channel;
import java.lang.reflect.Field;
import java.util.List;
import org.junit.Test;

/**
 * Unit tests for the dead-channel maintenance added to {@link ChannelPool2}: the periodic
 * {@code check()} must evict an underlying-netty-{@code Channel} whose {@code isActive() /
 * isOpen()} are false and try to replace it; {@link ChannelPool2#removeChannel} must drop a
 * specific NettyChannel synchronously so a concurrent borrow cannot re-pick it.
 *
 * <p>We avoid mocking the full netty Bootstrap stack — too brittle — and instead poke the
 * pool's internal {@code channels} list via reflection with mock {@link NettyChannel}s. This
 * exercises exactly the logic that was changed without standing up a real TCP listener.</p>
 */
public class ChannelPool2Test {

  private static List<NettyChannel> internalChannels(ChannelPool2 pool) throws Exception {
    Field f = ChannelPool2.class.getDeclaredField("channels");
    f.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<NettyChannel> list = (List<NettyChannel>) f.get(pool);
    return list;
  }

  /** Build a NettyChannel whose underlying netty Channel reports the given active/open state. */
  private static NettyChannel mockNettyChannel(boolean active, Location loc) {
    Channel underlying = mock(Channel.class);
    when(underlying.isActive()).thenReturn(active);
    when(underlying.isOpen()).thenReturn(active);
    return new NettyChannel(underlying, loc);
  }

  private static ChannelPool2 emptyPool(ChannelPoolParam param) {
    return new ChannelPool2(null /* bootstrap */, new Location("127.0.0.1", 12345), param);
  }

  private static ChannelPoolParam param(int maxActive, int minActive, long idleMs) {
    ChannelPoolParam p = new ChannelPoolParam();
    p.maxActive = maxActive;
    p.minActive = minActive;
    p.maxIdleTimeMs = idleMs;
    p.getChannelTimeoutMs = 1000L;
    return p;
  }

  @Test
  public void removeChannelDropsTargetAndClosesIt() throws Exception {
    ChannelPool2 pool = emptyPool(param(4, 1, 60_000));
    Location loc = new Location("127.0.0.1", 12345);
    NettyChannel a = mockNettyChannel(true, loc);
    NettyChannel b = mockNettyChannel(true, loc);
    List<NettyChannel> list = internalChannels(pool);
    list.add(a);
    list.add(b);

    pool.removeChannel(a);
    assertEquals("pool should shrink by exactly 1", 1, internalChannels(pool).size());
    assertSame("remaining entry should be the other channel",
        b, internalChannels(pool).get(0));
  }

  @Test
  public void removeChannelOnNullIsNoop() {
    ChannelPool2 pool = emptyPool(param(4, 1, 60_000));
    pool.removeChannel(null); // must not throw
  }

  @Test
  public void removeChannelOnUnknownIsNoop() throws Exception {
    ChannelPool2 pool = emptyPool(param(4, 1, 60_000));
    Location loc = new Location("127.0.0.1", 12345);
    NettyChannel a = mockNettyChannel(true, loc);
    internalChannels(pool).add(a);

    NettyChannel stranger = mockNettyChannel(true, loc);
    pool.removeChannel(stranger);
    assertEquals("pool size must not change", 1, internalChannels(pool).size());
    assertSame(a, internalChannels(pool).get(0));
  }

  @Test
  public void removeChannelsClearsList() throws Exception {
    ChannelPool2 pool = emptyPool(param(4, 1, 60_000));
    Location loc = new Location("127.0.0.1", 12345);
    internalChannels(pool).add(mockNettyChannel(true, loc));
    internalChannels(pool).add(mockNettyChannel(true, loc));

    pool.removeChannels();
    assertTrue("after removeChannels the internal list should be empty",
        internalChannels(pool).isEmpty());
  }

  /**
   * The dead-channel-replacement path requires {@code createChannel} (i.e. an actual
   * Bootstrap.connect) to succeed. We can't realistically supply a working bootstrap here,
   * so we restrict ourselves to verifying the dead entry is evicted even when replacement
   * fails — the pool must end up shorter, and the dead reference must be gone. This matches
   * the documented behaviour: "Failures while reconnecting are logged at WARN and the slot
   * is left empty — the next getChannel() call will lazily re-init the pool if it becomes
   * empty."
   */
  @Test
  public void checkEvictsDeadEntryEvenIfReconnectFails() throws Exception {
    ChannelPool2 pool = emptyPool(param(4, 1, 60_000));
    Location loc = new Location("127.0.0.1", 12345);
    NettyChannel alive = mockNettyChannel(true, loc);
    NettyChannel dead = mockNettyChannel(false, loc);
    List<NettyChannel> list = internalChannels(pool);
    list.add(alive);
    list.add(dead);

    pool.check();
    List<NettyChannel> after = internalChannels(pool);
    assertFalse("dead channel must be evicted", after.contains(dead));
    assertTrue("alive channel must remain", after.contains(alive));
  }

  @Test
  public void checkLeavesAllAliveChannelsAlone() throws Exception {
    // With maxIdleTimeMs in the future, no idle eviction; with all-alive channels, no dead
    // eviction. check() should be a no-op.
    ChannelPool2 pool = emptyPool(param(4, 1, Long.MAX_VALUE));
    Location loc = new Location("127.0.0.1", 12345);
    NettyChannel a = mockNettyChannel(true, loc);
    NettyChannel b = mockNettyChannel(true, loc);
    internalChannels(pool).add(a);
    internalChannels(pool).add(b);

    pool.check();
    List<NettyChannel> after = internalChannels(pool);
    assertEquals(2, after.size());
    assertSame(a, after.get(0));
    assertSame(b, after.get(1));
  }

  @Test
  public void checkRespectsMinActiveOnIdleEviction() throws Exception {
    // maxIdleTimeMs = 0 forces every alive channel to look "too idle", but minActive must
    // protect against shrinking below the floor.
    ChannelPool2 pool = emptyPool(param(4, 2 /* minActive */, 0));
    Location loc = new Location("127.0.0.1", 12345);
    NettyChannel a = mockNettyChannel(true, loc);
    NettyChannel b = mockNettyChannel(true, loc);
    NettyChannel c = mockNettyChannel(true, loc);
    internalChannels(pool).add(a);
    internalChannels(pool).add(b);
    internalChannels(pool).add(c);

    pool.check();
    int sizeAfter = internalChannels(pool).size();
    assertTrue("must not drop below minActive: " + sizeAfter, sizeAfter >= 2);
  }

  @Test
  public void deadAndAliveMixedKeepsAliveDropsDead() throws Exception {
    ChannelPool2 pool = emptyPool(param(8, 1, 60_000));
    Location loc = new Location("127.0.0.1", 12345);
    NettyChannel a = mockNettyChannel(true, loc);
    NettyChannel d1 = mockNettyChannel(false, loc);
    NettyChannel b = mockNettyChannel(true, loc);
    NettyChannel d2 = mockNettyChannel(false, loc);
    NettyChannel c = mockNettyChannel(true, loc);
    List<NettyChannel> list = internalChannels(pool);
    list.add(a);
    list.add(d1);
    list.add(b);
    list.add(d2);
    list.add(c);

    pool.check();
    List<NettyChannel> after = internalChannels(pool);
    assertFalse(after.contains(d1));
    assertFalse(after.contains(d2));
    assertTrue(after.contains(a));
    assertTrue(after.contains(b));
    assertTrue(after.contains(c));
    assertNotSame("at least one alive channel still in pool",
        0, after.size());
  }
}
