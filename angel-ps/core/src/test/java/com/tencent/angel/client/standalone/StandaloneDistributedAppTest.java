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
 * End-to-end smoke test for the STANDALONE <b>distributed</b> mode (route A: SSH-based
 * remote launcher). The test treats the loopback {@code localhost} as a remote host so that
 * the SSH path is exercised on a single machine.
 *
 * <p>Required environment to run this test:
 * <ul>
 *   <li>An SSH server running on this machine, accepting connections from the current user
 *       on {@code localhost}.</li>
 *   <li>Public-key (passwordless) authentication configured for {@code localhost}; OpenSSH's
 *       {@code BatchMode=yes} is forced and password prompts will fail the test.</li>
 *   <li>The {@code java} executable on PATH on the remote shell.</li>
 *   <li>{@code -Dangel.standalone.remote.classpath=...} pointing at a directory of Angel
 *       jars usable from the remote shell. For self-tests, the most convenient path is the
 *       {@code target/classes} of the current build plus the project's full dependency
 *       classpath; supply it via {@code -Dangel.standalone.remote.classpath=...}.</li>
 * </ul>
 *
 * <p>Disabled by default. Enable explicitly:
 * <pre>
 *   mvn -o test -Dtest=StandaloneDistributedAppTest \
 *       -Dangel.test.standalone.distributed.enabled=true \
 *       -Dangel.standalone.remote.classpath=$(cat /tmp/angel-cp.txt) \
 *       -DfailIfNoTests=false -pl angel-ps/core
 * </pre>
 */
public class StandaloneDistributedAppTest {

  private static final Log LOG = LogFactory.getLog(StandaloneDistributedAppTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");

  private AngelClient angelClient;

  @Before public void setup() throws Exception {
    Assume.assumeTrue(
        "Set -Dangel.test.standalone.distributed.enabled=true to run this end-to-end test.",
        Boolean.getBoolean("angel.test.standalone.distributed.enabled"));
    String remoteCp = System.getProperty("angel.standalone.remote.classpath");
    Assume.assumeTrue(
        "Set -Dangel.standalone.remote.classpath=... so the remote JVM can find Angel classes.",
        remoteCp != null && !remoteCp.isEmpty());

    Configuration conf = new Configuration();
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);

    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "STANDALONE");
    conf.set(AngelConf.ANGEL_RUNNING_MODE, RunningMode.ANGEL_PS_WORKER.toString());
    conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true);
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());

    String runDir = TMP_PATH + File.separator + "angel-standalone-dist-test";
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + runDir + "/out");
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, LOCAL_FS + runDir + "/in");
    conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + runDir + "/log");
    conf.set(AngelConf.ANGEL_STAGING_DIR, LOCAL_FS + runDir + "/staging");
    conf.set(AngelConf.ANGEL_STANDALONE_LOG_DIR, runDir + "/standalone-logs");

    // Treat 127.0.0.1 as a "remote" host -- this exercises the ssh + scp code paths.
    conf.set(AngelConf.ANGEL_STANDALONE_HOSTS, "127.0.0.1");
    conf.set(AngelConf.ANGEL_STANDALONE_REMOTE_CLASSPATH, remoteCp);
    conf.set(AngelConf.ANGEL_STANDALONE_REMOTE_LOG_DIR, runDir + "/remote-logs");

    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_WORKER_HEARTBEAT_INTERVAL_MS, 1000);
    conf.setInt(AngelConf.ANGEL_PS_HEARTBEAT_INTERVAL_MS, 1000);

    conf.set(AngelConf.ANGEL_PS_JAVA_OPTS, "-Xmx512m");
    conf.set(AngelConf.ANGEL_WORKER_JAVA_OPTS, "-Xmx512m");

    angelClient = AngelClientFactory.get(conf);
    assertTrue(angelClient instanceof AngelStandaloneClient);

    angelClient.startPSServer();
    angelClient.runTask(DummyTask.class);
    Thread.sleep(25000);
  }

  @Test public void testRemotePSComesUp() throws Exception {
    if (angelClient == null) return;
    Location loc = ((AngelStandaloneClient) angelClient).getMasterLocation();
    assertNotNull(loc);
    LOG.info("distributed-standalone master at " + loc);

    GetAllPSLocationResponse resp = ((AngelStandaloneClient) angelClient)
        .getMasterServiceForTest()
        .getAllPSLocation(null, GetAllPSLocationRequest.newBuilder().build());
    boolean anyOk = false;
    for (PSLocationProto p : resp.getPsLocationsList()) {
      LOG.info("PS " + p.getPsId() + " status=" + p.getPsStatus());
      if (p.getPsStatus() == PSStatus.PS_OK) {
        anyOk = true;
      }
    }
    assertTrue("at least one ssh-launched PS should report PS_OK", anyOk);
  }

  @After public void stop() throws Exception {
    if (angelClient != null) {
      try {
        angelClient.stop();
      } catch (Exception x) {
        LOG.error("stop failed ", x);
      }
    }
  }
}
