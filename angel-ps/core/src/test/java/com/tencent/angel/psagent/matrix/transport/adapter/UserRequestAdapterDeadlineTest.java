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
package com.tencent.angel.psagent.matrix.transport.adapter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

/**
 * Tests for {@link UserRequestAdapter#findExpiredRequestIds(Map, long, long)} —
 * the pure-function core of the user-request deadline watchdog. We bypass the adapter
 * itself (which depends on a fully wired {@code PSAgentContext}) and exercise the
 * detection contract directly, since that's where the bug-prevention value lives.
 *
 * <p>The contract under test:
 * <ul>
 *   <li>requests with {@code (now - createTs) <= deadline} are NOT flagged;</li>
 *   <li>requests with {@code (now - createTs) > deadline} ARE flagged;</li>
 *   <li>boundary case {@code age == deadline} is NOT flagged (strict inequality);</li>
 *   <li>{@code deadline <= 0} disables the watchdog entirely;</li>
 *   <li>empty map is a no-op;</li>
 *   <li>multiple expired requests are all flagged in a single pass.</li>
 * </ul>
 */
public class UserRequestAdapterDeadlineTest {

  /**
   * Override the (final) {@code createTs} field of a freshly constructed UserRequest
   * with a deterministic value so the deadline arithmetic is testable.
   */
  private static UserRequest reqAt(long createTs) throws Exception {
    UserRequest r = new GetRowRequest(0, 0);
    Field f = UserRequest.class.getDeclaredField("createTs");
    f.setAccessible(true);
    // For final long fields, reflection on a non-static field can still call setLong.
    f.setLong(r, createTs);
    return r;
  }

  @Test
  public void emptyMapIsNoop() {
    List<Integer> expired = UserRequestAdapter.findExpiredRequestIds(
        Collections.<Integer, UserRequest>emptyMap(), 1000L, 9999L);
    assertTrue(expired.isEmpty());
  }

  @Test
  public void zeroOrNegativeDeadlineDisablesWatchdog() throws Exception {
    Map<Integer, UserRequest> map = new HashMap<>();
    map.put(1, reqAt(0L));  // 100% expired by any positive deadline
    long now = 10_000L;

    assertTrue("deadline=0 must be a no-op",
        UserRequestAdapter.findExpiredRequestIds(map, 0L, now).isEmpty());
    assertTrue("negative deadline must be a no-op",
        UserRequestAdapter.findExpiredRequestIds(map, -1L, now).isEmpty());
  }

  @Test
  public void freshRequestIsNotExpired() throws Exception {
    Map<Integer, UserRequest> map = new HashMap<>();
    map.put(7, reqAt(9_500L));
    List<Integer> expired = UserRequestAdapter.findExpiredRequestIds(map, 1000L, 10_000L);
    assertTrue("a 500ms-old request under a 1000ms deadline must not be flagged",
        expired.isEmpty());
  }

  @Test
  public void exactlyAtDeadlineIsNotExpired() throws Exception {
    // The contract uses strict ">", so age == deadline is allowed (one tick of grace).
    Map<Integer, UserRequest> map = new HashMap<>();
    map.put(7, reqAt(9_000L));
    List<Integer> expired = UserRequestAdapter.findExpiredRequestIds(map, 1000L, 10_000L);
    assertTrue("age == deadline must NOT be flagged (strict >)", expired.isEmpty());
  }

  @Test
  public void oneTickPastDeadlineIsExpired() throws Exception {
    Map<Integer, UserRequest> map = new HashMap<>();
    map.put(7, reqAt(8_999L));
    List<Integer> expired = UserRequestAdapter.findExpiredRequestIds(map, 1000L, 10_000L);
    assertEquals(1, expired.size());
    assertEquals(Integer.valueOf(7), expired.get(0));
  }

  @Test
  public void multipleRequestsAtMixedAges() throws Exception {
    Map<Integer, UserRequest> map = new HashMap<>();
    map.put(1, reqAt(9_900L));   // 100ms old, fresh
    map.put(2, reqAt(8_500L));   // 1500ms old, EXPIRED
    map.put(3, reqAt(9_500L));   // 500ms old, fresh
    map.put(4, reqAt(7_000L));   // 3000ms old, EXPIRED
    long now = 10_000L;

    List<Integer> expired = UserRequestAdapter.findExpiredRequestIds(map, 1000L, now);
    Set<Integer> got = new HashSet<>(expired);
    Set<Integer> want = new HashSet<>(Arrays.asList(2, 4));
    assertEquals("only the past-deadline ids must be flagged", want, got);
  }

  /**
   * Sanity: a fresh request created via the public constructor (no reflection) reports a
   * {@code createTs} close to {@code System.currentTimeMillis()} and is therefore not
   * flagged by a positive deadline. Catches accidental reordering of the field
   * initialiser or a typo in {@link UserRequest#getCreateTs()}.
   */
  @Test
  public void publiclyConstructedRequestStartsAtNow() {
    long before = System.currentTimeMillis();
    UserRequest r = new GetRowRequest(0, 0);
    long after = System.currentTimeMillis();
    long createTs = r.getCreateTs();
    assertTrue("createTs must lie within [before, after] of construction",
        createTs >= before && createTs <= after);

    Map<Integer, UserRequest> map = new HashMap<>();
    map.put(1, r);
    // A 60-second deadline against a freshly-constructed request must never expire.
    List<Integer> expired = UserRequestAdapter.findExpiredRequestIds(
        map, 60_000L, System.currentTimeMillis());
    assertFalse(expired.contains(1));
  }
}
