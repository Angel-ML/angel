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

package com.tencent.angel.client.attach;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.tencent.angel.RunningMode;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.client.standalone.AngelStandaloneClient;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.model.MatrixSaveContext;
import com.tencent.angel.model.ModelSaveContext;
import com.tencent.angel.ps.storage.partitioner.RangePartitioner;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

/**
 * End-to-end test that spins up a STANDALONE Angel master+PS in the test JVM (the same way
 * {@code bin/angel-cluster-up} would on a remote host), then connects an
 * {@link AngelAttachClient} to that master and exercises the control-plane operations a
 * downstream computing engine (e.g. Spark) would invoke:
 *
 * <ol>
 *   <li>create matrices via the attach client</li>
 *   <li>save those matrices to a local-FS path</li>
 *   <li>{@link AngelClient#waitForAllPS(int) waitForAllPS} against the same master</li>
 *   <li>detach without bringing the cluster down (verified by the "owner" client still
 *       being able to talk to the master afterwards)</li>
 * </ol>
 *
 * <p>Like {@code StandaloneAppTest}, this test spawns child JVMs and is only run when
 * explicitly enabled via:
 * <pre>
 *   mvn -o test -Dangel.test.standalone.enabled=true \
 *       -Dtest=AngelAttachClientTest \
 *       -pl angel-ps/core
 * </pre>
 */
public class AngelAttachClientTest {

  private static final Log LOG = LogFactory.getLog(AngelAttachClientTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");

  /** Long-running cluster fixture (master + PS), owned by the test. */
  private AngelClient ownerClient;
  /** Master location of the fixture, used by the attach client. */
  private Location masterLoc;
  /** Save path for {@link #testCreateMatricesAndSave()}. */
  private String runDir;

  @Before public void setup() throws Exception {
    Assume.assumeTrue(
        "Set -Dangel.test.standalone.enabled=true to run AngelAttachClientTest end-to-end.",
        Boolean.getBoolean("angel.test.standalone.enabled"));

    Configuration conf = new Configuration();
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);

    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "STANDALONE");
    // PS-only fixture: that's all an external engine needs to attach to.
    conf.set(AngelConf.ANGEL_RUNNING_MODE, RunningMode.ANGEL_PS.toString());
    conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true);
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());

    runDir = TMP_PATH + File.separator + "angel-attach-test-" + System.currentTimeMillis();
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + runDir + "/owner-out");
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, LOCAL_FS + runDir + "/in");
    conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + runDir + "/log");
    conf.set(AngelConf.ANGEL_STAGING_DIR, LOCAL_FS + runDir + "/staging");
    conf.set(AngelConf.ANGEL_STANDALONE_LOG_DIR, runDir + "/standalone-logs");

    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 0);
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_PS_HEARTBEAT_INTERVAL_MS, 1000);

    // Keep child JVM heap small so the test is friendly in CI.
    conf.set(AngelConf.ANGEL_PS_JAVA_OPTS, "-Xmx512m");

    ownerClient = AngelClientFactory.get(conf);
    assertTrue("expected AngelStandaloneClient for STANDALONE mode",
        ownerClient instanceof AngelStandaloneClient);

    LOG.info("starting standalone master+PS cluster ...");
    ownerClient.startPSServer();
    masterLoc = ((AngelStandaloneClient) ownerClient).getMasterLocation();
    assertNotNull("master location must be available after startPSServer", masterLoc);

    // Wait until at least one PS reports ready: this is what an external engine would do
    // before issuing createMatrices / save against it.
    ownerClient.waitForAllPS(1);
    LOG.info("master is up at " + masterLoc + ", PS is ready.");
  }

  /**
   * Attach with a configuration-only constructor (the realistic Spark-on-Angel code path:
   * the master {@code host:port} is propagated through {@code spark.hadoop.angel.master.*}
   * and ends up in a Hadoop {@link Configuration}).
   */
  @Test public void testAttachByConfigOnly() throws Exception {
    if (ownerClient == null) {
      return; // assumed away
    }

    Configuration conf = new Configuration();
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
    conf.set(AngelAttachClient.ANGEL_MASTER_HOST, masterLoc.getIp());
    conf.setInt(AngelAttachClient.ANGEL_MASTER_PORT, masterLoc.getPort());
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);

    AngelAttachClient attach = new AngelAttachClient(conf);
    try {
      attach.startPSServer();
      assertEquals("default external app id", "external-angel-ps", attach.getAppIdString());
      // waitForAllPS should resolve immediately because the fixture is already ready.
      attach.waitForAllPS(1);
    } finally {
      attach.stop();
    }
  }

  /**
   * The main scenario: create matrices via the attach client, save them, then ensure
   * detach is non-destructive (the cluster keeps serving the original owner client).
   */
  @Test public void testCreateMatricesAndSave() throws Exception {
    if (ownerClient == null) {
      return; // assumed away
    }

    AngelAttachClient attach = new AngelAttachClient(new Configuration(), masterLoc);
    String savePath = LOCAL_FS + runDir + "/attach-out";

    try {
      attach.startPSServer();

      // 1) create two matrices: one dense, one sparse-by-range. Dense matrices must use
      // RangePartitioner explicitly because MatrixContext defaults to HashPartitioner,
      // which Angel forbids for dense row types.
      MatrixContext dense = new MatrixContext("attach_dense", 4, 100, -1, -1, -1, 2, 50,
          new java.util.ArrayList<>(), RowType.T_DOUBLE_DENSE);
      dense.setPartitionerClass(RangePartitioner.class);
      MatrixContext sparse = new MatrixContext("attach_sparse", 1, 1024, 1024, -1, -1, 1, 256,
          new java.util.ArrayList<>(), RowType.T_FLOAT_SPARSE);
      sparse.setPartitionerClass(RangePartitioner.class);
      attach.createMatrices(Arrays.asList(dense, sparse));

      // 2) save them to local-FS.
      ModelSaveContext save = new ModelSaveContext();
      save.setSavePath(savePath);
      save.addMatrix(new MatrixSaveContext(dense.getName()));
      save.addMatrix(new MatrixSaveContext(sparse.getName()));
      attach.save(save);

      // The save() call is synchronous: the master returns SUCCESS only after every
      // matrix has been committed under savePath. Verify the directory is non-empty.
      Path saveDir = new Path(savePath);
      FileSystem fs = saveDir.getFileSystem(attach.getConf());
      assertTrue("save path " + saveDir + " must exist after save()", fs.exists(saveDir));
      assertTrue("save path " + saveDir + " must contain matrix files",
          fs.listStatus(saveDir).length > 0);
    } finally {
      attach.stop();
    }

    // 3) Detach must NOT have stopped the cluster: the owner client should still be able
    // to talk to the master.
    int psNum = ((AngelStandaloneClient) ownerClient).getConf()
        .getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER);
    try {
      ownerClient.waitForAllPS(psNum);
    } catch (Exception e) {
      fail("owner client lost contact with master after attach client detached: " + e);
    }
  }

  /**
   * Save with an empty {@link ModelSaveContext} must be rejected client-side, so the
   * master is never asked to create a malformed save request.
   */
  @Test public void testSaveRejectsEmptyContext() throws Exception {
    if (ownerClient == null) {
      return; // assumed away
    }

    AngelAttachClient attach = new AngelAttachClient(new Configuration(), masterLoc);
    try {
      attach.startPSServer();
      ModelSaveContext empty = new ModelSaveContext();
      empty.setSavePath(LOCAL_FS + runDir + "/never-written");
      try {
        attach.save(empty);
        fail("save() with no matrices should throw AngelException");
      } catch (AngelException expected) {
        assertNotNull(expected.getMessage());
      }
    } finally {
      attach.stop();
    }
  }

  /**
   * Constructor with no master in conf and no explicit Location must fail fast.
   */
  @Test public void testMissingMasterRejected() {
    Configuration empty = new Configuration();
    try {
      new AngelAttachClient(empty);
      fail("attach client without master location should throw");
    } catch (IllegalArgumentException expected) {
      assertTrue(expected.getMessage().contains("Master location is null"));
    }
  }

  @After public void tearDown() throws Exception {
    if (ownerClient != null) {
      try {
        LOG.info("stopping standalone fixture cluster");
        ownerClient.stop();
      } catch (Throwable t) {
        LOG.error("stop owner client failed", t);
      }
    }
    // Best-effort cleanup of the local-FS scratch directory.
    if (runDir != null) {
      try {
        Path p = new Path(LOCAL_FS + runDir);
        FileSystem fs = p.getFileSystem(new Configuration());
        if (fs.exists(p)) fs.delete(p, true);
      } catch (Throwable ignore) {
      }
    }
    // Silence unused-import warnings under -Werror by referencing this list once.
    @SuppressWarnings("unused")
    java.util.List<?> noop = Collections.emptyList();
  }
}
