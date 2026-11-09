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

package com.tencent.angel.ps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.tencent.angel.RunningMode;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.client.standalone.AngelStandaloneClient;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.AngelApplicationMaster;
import com.tencent.angel.master.deploy.standalone.StandaloneContainerLauncher;
import java.io.File;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end test for the STANDALONE PS self-kill-on-master-loss path.
 *
 * <p>Brings up a STANDALONE master + 1 PS, then forcibly stops the master's RPC server so
 * that subsequent PS heartbeats fail without the master cooperating in the shutdown
 * (i.e. without the {@link StandaloneContainerLauncher} destroying the child process
 * itself). Verifies the PS child JVM exits on its own after the configured number of
 * consecutive failed heartbeats.
 *
 * <p>Like other end-to-end tests in this module, this one is gated by
 * {@code -Dangel.test.standalone.enabled=true}.
 */
public class StandalonePSMasterLossTest {

  private static final Log LOG = LogFactory.getLog(StandalonePSMasterLossTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");

  /** Heartbeat tuning chosen so the PS detects loss within ~5 s instead of ~5 min. */
  private static final int HB_INTERVAL_MS = 500;
  private static final int FAILURE_TOLERATE = 3;

  private AngelClient ownerClient;
  private Location masterLoc;
  private String runDir;

  @Before public void setup() throws Exception {
    Assume.assumeTrue(
        "Set -Dangel.test.standalone.enabled=true to run StandalonePSMasterLossTest end-to-end.",
        Boolean.getBoolean("angel.test.standalone.enabled"));

    Configuration conf = new Configuration();
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);

    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "STANDALONE");
    conf.set(AngelConf.ANGEL_RUNNING_MODE, RunningMode.ANGEL_PS.toString());
    conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true);
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());

    runDir = TMP_PATH + File.separator + "angel-ps-master-loss-test-" + System.currentTimeMillis();
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + runDir + "/out");
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, LOCAL_FS + runDir + "/in");
    conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + runDir + "/log");
    conf.set(AngelConf.ANGEL_STAGING_DIR, LOCAL_FS + runDir + "/staging");
    conf.set(AngelConf.ANGEL_STANDALONE_LOG_DIR, runDir + "/standalone-logs");

    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 0);
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_PS_HEARTBEAT_INTERVAL_MS, HB_INTERVAL_MS);
    // The whole point of this test: trigger the PS-side master-loss kill quickly.
    conf.setInt(AngelConf.ANGEL_PS_HEARTBEAT_FAILURE_TOLERATE_COUNT, FAILURE_TOLERATE);

    conf.set(AngelConf.ANGEL_PS_JAVA_OPTS, "-Xmx512m");

    ownerClient = AngelClientFactory.get(conf);
    assertTrue("expected AngelStandaloneClient for STANDALONE mode",
        ownerClient instanceof AngelStandaloneClient);

    LOG.info("starting standalone master+PS cluster ...");
    ownerClient.startPSServer();
    masterLoc = ((AngelStandaloneClient) ownerClient).getMasterLocation();
    assertNotNull(masterLoc);
    ownerClient.waitForAllPS(1);
    LOG.info("master is up at " + masterLoc + ", PS is ready.");
  }

  /**
   * Kill only the master's RPC listener (without going through the AM finish path that
   * would itself destroy child processes) and verify that the PS child JVM exits on its
   * own after {@link AngelConf#ANGEL_PS_HEARTBEAT_FAILURE_TOLERATE_COUNT} consecutive
   * failed heartbeats.
   */
  @Test public void testPSExitsAfterMasterRpcServerStops() throws Exception {
    if (ownerClient == null) {
      return; // assumed away
    }

    AngelApplicationMaster am = ((AngelStandaloneClient) ownerClient).getAppMasterForTest();
    assertNotNull("standalone client must expose its embedded AM", am);

    StandaloneContainerLauncher launcher = (StandaloneContainerLauncher) am.getContainerLauncher();
    assertNotNull(launcher);
    assertEquals("expected exactly 1 alive PS child before master loss",
        1, launcher.aliveChildCount());

    // Forcibly kill the master's RPC server. We deliberately do NOT call
    // AngelApplicationMaster.shutDownJob() / MasterService.stop(int): those would also
    // tear down the container launcher, which would destroy the PS child process and
    // therefore mask the very behaviour we want to observe.
    LOG.info("stopping master RPC server to simulate master loss ...");
    am.getAppContext().getMasterService().getRpcServer().stop();

    // Detection budget: FAILURE_TOLERATE failed heartbeats spaced HB_INTERVAL_MS apart,
    // plus a generous grace period for the PS JVM to actually exit. The deadline is the
    // upper bound; we poll well below it.
    long detectionDeadline = System.currentTimeMillis()
        + (long) FAILURE_TOLERATE * HB_INTERVAL_MS + 15_000L;
    int alive = launcher.aliveChildCount();
    while (alive > 0 && System.currentTimeMillis() < detectionDeadline) {
      Thread.sleep(500);
      alive = launcher.aliveChildCount();
    }

    assertEquals("PS child should have exited after master loss within "
        + (FAILURE_TOLERATE * HB_INTERVAL_MS + 15_000) + " ms, but " + alive + " still alive",
        0, alive);
  }

  @After public void tearDown() throws Exception {
    if (ownerClient != null) {
      // The RPC server is already stopped at this point; AngelClient.stop() will fall
      // through to kill() since the master proxy is unreachable. That is fine.
      try {
        ownerClient.stop();
      } catch (Throwable t) {
        LOG.warn("owner client stop failed (expected after master rpc shutdown)", t);
      }
    }
  }
}
