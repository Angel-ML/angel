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

package com.tencent.angel.client.standalone;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.deploy.standalone.StandaloneHostPool;
import com.tencent.angel.master.deploy.standalone.StandaloneRemoteLauncher;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerGroupId;
import com.tencent.angel.worker.WorkerId;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * Pure unit tests for the multi-host scheduling pieces of STANDALONE distributed mode.
 * These tests do not contact any real network or invoke ssh.
 */
public class StandaloneHostPoolTest {

  private static PSAttemptId psAttempt(int psIndex, int attemptIndex) {
    return new PSAttemptId(new ParameterServerId(psIndex), attemptIndex);
  }

  private static WorkerAttemptId workerAttempt(int groupIndex, int workerIndex, int attemptIndex) {
    return new WorkerAttemptId(
        new WorkerId(new WorkerGroupId(groupIndex), workerIndex), attemptIndex);
  }

  @Test public void testEmptyConfigIsSingleNode() {
    Configuration conf = new Configuration();
    StandaloneHostPool pool = new StandaloneHostPool(conf);
    assertTrue(pool.isSingleNode());
    StandaloneHostPool.Target t = pool.assign(psAttempt(0, 0));
    assertNotNull(t);
    assertTrue("empty pool should yield a local target", t.local);
  }

  @Test public void testGlobalHostsRoundRobinForPS() {
    Configuration conf = new Configuration();
    conf.set(AngelConf.ANGEL_STANDALONE_HOSTS, "h1,h2,h3");
    StandaloneHostPool pool = new StandaloneHostPool(conf);
    assertFalse(pool.isSingleNode());

    StandaloneHostPool.Target t1 = pool.assign(psAttempt(0, 0));
    StandaloneHostPool.Target t2 = pool.assign(psAttempt(1, 0));
    StandaloneHostPool.Target t3 = pool.assign(psAttempt(2, 0));
    StandaloneHostPool.Target t4 = pool.assign(psAttempt(3, 0));
    assertEquals("h1", t1.host);
    assertEquals("h2", t2.host);
    assertEquals("h3", t3.host);
    assertEquals("h1", t4.host);   // wrap-around
  }

  @Test public void testRoleSpecificOverrides() {
    Configuration conf = new Configuration();
    conf.set(AngelConf.ANGEL_STANDALONE_HOSTS, "shared1,shared2");
    conf.set(AngelConf.ANGEL_STANDALONE_PS_HOSTS, "ps-only-1,ps-only-2");
    conf.set(AngelConf.ANGEL_STANDALONE_WORKER_HOSTS, "w-only-1");
    StandaloneHostPool pool = new StandaloneHostPool(conf);

    assertEquals("ps-only-1", pool.assign(psAttempt(0, 0)).host);
    assertEquals("ps-only-2", pool.assign(psAttempt(1, 0)).host);
    assertEquals("w-only-1", pool.assign(workerAttempt(0, 0, 0)).host);
    assertEquals("w-only-1", pool.assign(workerAttempt(0, 1, 0)).host);
  }

  @Test public void testPerHostPortOverride() {
    Configuration conf = new Configuration();
    conf.set(AngelConf.ANGEL_STANDALONE_HOSTS, "h1:2222,h2");
    conf.setInt(AngelConf.ANGEL_STANDALONE_SSH_PORT, 22);
    StandaloneHostPool pool = new StandaloneHostPool(conf);

    StandaloneHostPool.Target t1 = pool.assign(psAttempt(0, 0));
    StandaloneHostPool.Target t2 = pool.assign(psAttempt(1, 0));
    assertEquals("h1", t1.host);
    assertEquals(2222, t1.port);
    assertEquals("h2", t2.host);
    assertEquals(22, t2.port);
  }

  @Test public void testLocalhostMarkedAsLocal() {
    Configuration conf = new Configuration();
    conf.set(AngelConf.ANGEL_STANDALONE_HOSTS, "localhost,127.0.0.1");
    StandaloneHostPool pool = new StandaloneHostPool(conf);
    StandaloneHostPool.Target t = pool.assign(psAttempt(0, 0));
    assertTrue("localhost should resolve to a local target", t.local);
  }

  @Test public void testAssignmentStableForSameId() {
    Configuration conf = new Configuration();
    conf.set(AngelConf.ANGEL_STANDALONE_HOSTS, "h1,h2,h3");
    StandaloneHostPool pool = new StandaloneHostPool(conf);

    PSAttemptId id = psAttempt(7, 0);
    StandaloneHostPool.Target a = pool.assign(id);
    StandaloneHostPool.Target b = pool.assign(id);
    assertSame("assigning the same id twice must return the cached target", a, b);

    pool.release(id);
    StandaloneHostPool.Target c = pool.assign(id);
    assertNotSame("after release, a fresh allocation is expected", a, c);
  }

  // ---------------------------------------------------------------------------------------------
  // multiplier syntax  ("ip*N" / "ip:port*N")

  @Test public void testMultiplierExpandsSingleHost() {
    Configuration conf = new Configuration();
    // Three PS slots on the same machine via the "*N" multiplier.
    conf.set(AngelConf.ANGEL_STANDALONE_PS_HOSTS, "10.0.0.1*3");
    StandaloneHostPool pool = new StandaloneHostPool(conf);

    assertEquals("10.0.0.1", pool.assign(psAttempt(0, 0)).host);
    assertEquals("10.0.0.1", pool.assign(psAttempt(1, 0)).host);
    assertEquals("10.0.0.1", pool.assign(psAttempt(2, 0)).host);
    // A fourth id still wraps around to the first (and only) host.
    assertEquals("10.0.0.1", pool.assign(psAttempt(3, 0)).host);
  }

  @Test public void testMultiplierMixedWithPlainEntries() {
    Configuration conf = new Configuration();
    // 3 PSes on host h1, 1 on h2 — asymmetric layout.
    conf.set(AngelConf.ANGEL_STANDALONE_PS_HOSTS, "h1*3,h2");
    StandaloneHostPool pool = new StandaloneHostPool(conf);

    assertEquals("h1", pool.assign(psAttempt(0, 0)).host);
    assertEquals("h1", pool.assign(psAttempt(1, 0)).host);
    assertEquals("h1", pool.assign(psAttempt(2, 0)).host);
    assertEquals("h2", pool.assign(psAttempt(3, 0)).host);
    // Wraps back to h1 — confirms pool size == 4 (3 + 1), not 2.
    assertEquals("h1", pool.assign(psAttempt(4, 0)).host);
  }

  @Test public void testMultiplierPreservesPort() {
    Configuration conf = new Configuration();
    conf.set(AngelConf.ANGEL_STANDALONE_PS_HOSTS, "h1:2222*2,h2");
    conf.setInt(AngelConf.ANGEL_STANDALONE_SSH_PORT, 22);
    StandaloneHostPool pool = new StandaloneHostPool(conf);

    StandaloneHostPool.Target t1 = pool.assign(psAttempt(0, 0));
    StandaloneHostPool.Target t2 = pool.assign(psAttempt(1, 0));
    StandaloneHostPool.Target t3 = pool.assign(psAttempt(2, 0));
    assertEquals("h1", t1.host);
    assertEquals(2222, t1.port);
    assertEquals("h1", t2.host);
    assertEquals(2222, t2.port);
    assertEquals("h2", t3.host);
    assertEquals(22, t3.port);
  }

  @Test public void testMultiplierZeroDropsEntry() {
    Configuration conf = new Configuration();
    // "*0" explicitly removes the entry; only h2 remains.
    conf.set(AngelConf.ANGEL_STANDALONE_PS_HOSTS, "h1*0,h2");
    StandaloneHostPool pool = new StandaloneHostPool(conf);

    assertEquals("h2", pool.assign(psAttempt(0, 0)).host);
    assertEquals("h2", pool.assign(psAttempt(1, 0)).host); // wraps, pool size = 1
  }

  @Test public void testMultiplierGarbageFallsBack() {
    Configuration conf = new Configuration();
    // "*abc" is not a number: the whole string must remain the host name, not silently
    // become "h1" with a fictional multiplier.
    conf.set(AngelConf.ANGEL_STANDALONE_PS_HOSTS, "h1*abc");
    StandaloneHostPool pool = new StandaloneHostPool(conf);

    StandaloneHostPool.Target t = pool.assign(psAttempt(0, 0));
    assertEquals("h1*abc", t.host);
  }

  // ---------------------------------------------------------------------------------------------
  // shell quoting

  @Test public void testShellQuotePlain() {
    assertEquals("'abc'", StandaloneRemoteLauncher.shellQuote("abc"));
  }

  @Test public void testShellQuoteWithSpecialChars() {
    // Embedded single quote should be rendered as '\'' (close, escaped, reopen).
    assertEquals("'a'\\''b'", StandaloneRemoteLauncher.shellQuote("a'b"));
    // Spaces / dollars / semicolons must remain inside the literal quoted region.
    assertEquals("'echo $HOME; rm -rf /'",
        StandaloneRemoteLauncher.shellQuote("echo $HOME; rm -rf /"));
  }

  @Test public void testShellQuoteNull() {
    assertEquals("''", StandaloneRemoteLauncher.shellQuote(null));
  }
}
