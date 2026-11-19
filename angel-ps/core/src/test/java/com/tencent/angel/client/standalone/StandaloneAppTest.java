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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.tencent.angel.RunningMode;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.DummyTask;
import com.tencent.angel.protobuf.generated.MLProtos.GetAllPSLocationRequest;
import com.tencent.angel.protobuf.generated.MLProtos.GetAllPSLocationResponse;
import com.tencent.angel.protobuf.generated.MLProtos.PSLocationProto;
import com.tencent.angel.protobuf.generated.MLProtos.PSStatus;
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
 * End-to-end smoke test for the STANDALONE deploy mode.
 *
 * <p>This test embeds an Angel ApplicationMaster in the test JVM and lets it spawn local
 * Parameter Server / Worker JVM processes via {@link
 * com.tencent.angel.master.deploy.standalone.StandaloneContainerLauncher}. It verifies that
 * the spawned PS registers with the master and the master can serve PS-location queries.
 *
 * <p>Because spawning child JVMs depends on the surefire-provided classpath and a usable
 * loopback network, this test is gated by the {@code angel.test.standalone.enabled} system
 * property. To run it explicitly:
 * <pre>
 *   mvn -o test -Dangel.test.standalone.enabled=true \
 *       -Dtest=StandaloneAppTest \
 *       -pl angel-ps/core
 * </pre>
 */
public class StandaloneAppTest {

  private static final Log LOG = LogFactory.getLog(StandaloneAppTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");

  private AngelClient angelClient;

  @Before public void setup() throws Exception {
    Assume.assumeTrue(
        "Set -Dangel.test.standalone.enabled=true to run StandaloneAppTest end-to-end.",
        Boolean.getBoolean("angel.test.standalone.enabled"));

    Configuration conf = new Configuration();
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);

    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "STANDALONE");
    conf.set(AngelConf.ANGEL_RUNNING_MODE, RunningMode.ANGEL_PS_WORKER.toString());
    conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true);
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());

    String runDir = TMP_PATH + File.separator + "angel-standalone-test";
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + runDir + "/out");
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, LOCAL_FS + runDir + "/in");
    conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + runDir + "/log");
    conf.set(AngelConf.ANGEL_STAGING_DIR, LOCAL_FS + runDir + "/staging");
    conf.set(AngelConf.ANGEL_STANDALONE_LOG_DIR, runDir + "/standalone-logs");

    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_WORKER_HEARTBEAT_INTERVAL_MS, 1000);
    conf.setInt(AngelConf.ANGEL_PS_HEARTBEAT_INTERVAL_MS, 1000);

    // Make sure the spawned JVMs don't allocate huge heaps in CI.
    conf.set(AngelConf.ANGEL_PS_JAVA_OPTS, "-Xmx512m");
    conf.set(AngelConf.ANGEL_WORKER_JAVA_OPTS, "-Xmx512m");

    angelClient = AngelClientFactory.get(conf);
    assertTrue(
        "factory should produce AngelStandaloneClient for STANDALONE mode",
        angelClient instanceof AngelStandaloneClient);

    angelClient.startPSServer();
    angelClient.runTask(DummyTask.class);
    // Give child JVMs enough time to boot, register and reach RUNNING state.
    Thread.sleep(20000);
  }

  @Test public void testPSAndWorkerComeUp() throws Exception {
    if (angelClient == null) {
      // Test was assumed away in setup; nothing to assert.
      return;
    }

    Location masterLoc = ((AngelStandaloneClient) angelClient).getMasterLocation();
    assertNotNull("master location must be available after startPSServer", masterLoc);
    LOG.info("standalone master location = " + masterLoc);

    // Query the master directly via the established RPC client.
    GetAllPSLocationResponse resp = ((AngelStandaloneClient) angelClient).getMasterServiceForTest()
        .getAllPSLocation(null, GetAllPSLocationRequest.newBuilder().build());
    assertNotNull(resp);
    assertTrue("expected at least 1 PS location, got " + resp.getPsLocationsList(),
        resp.getPsLocationsList().size() >= 1);

    boolean anyReady = false;
    for (PSLocationProto p : resp.getPsLocationsList()) {
      LOG.info("PS " + p.getPsId() + " status=" + p.getPsStatus());
      if (p.getPsStatus() == PSStatus.PS_OK) {
        anyReady = true;
      }
    }
    assertTrue("at least one spawned PS process should report PS_OK", anyReady);
  }

  @After public void stop() throws Exception {
    if (angelClient != null) {
      try {
        LOG.info("stopping standalone cluster");
        angelClient.stop();
      } catch (Exception x) {
        LOG.error("stop failed ", x);
      }
    }
  }
}
