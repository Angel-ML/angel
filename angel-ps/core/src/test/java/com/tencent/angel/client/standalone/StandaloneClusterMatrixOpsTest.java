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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.tencent.angel.client.attach.AngelAttachClient;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ml.matrix.psf.update.zero.Zero;
import com.tencent.angel.model.MatrixLoadContext;
import com.tencent.angel.model.MatrixSaveContext;
import com.tencent.angel.model.ModelLoadContext;
import com.tencent.angel.model.ModelSaveContext;
import com.tencent.angel.ps.storage.partitioner.HashPartitioner;
import com.tencent.angel.psagent.PSAgent;
import com.tencent.angel.psagent.matrix.MatrixClient;
import java.io.File;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Data-plane test for the long-running <b>PS cluster</b> mode.
 *
 * <p>A separate JVM runs {@link com.tencent.angel.utils.AngelPSClusterMain} (exactly what
 * {@code bin/angel-cluster-up} starts) with two PS processes. The test JVM plays the role of
 * an external computing engine: it attaches with {@link AngelAttachClient} for control-plane
 * calls (create / save / load matrices) and uses a {@link PSAgent} for data-plane calls.
 *
 * <p>Spawns child JVMs, so it only runs when explicitly enabled:
 * <pre>
 *   mvn -o test -Dangel.test.standalone.enabled=true \
 *       -Dtest=StandaloneClusterMatrixOpsTest -pl angel-ps/core
 * </pre>
 */
public class StandaloneClusterMatrixOpsTest {

  private static final Log LOG = LogFactory.getLog(StandaloneClusterMatrixOpsTest.class);

  private static final int PS_NUM = 2;
  private static final double EPS = 1e-6;

  private static File runDir;
  private static StandaloneTestSupport.ExternalCluster cluster;
  private static AngelAttachClient attach;
  private static PSAgent psAgent;

  @BeforeClass public static void setUpCluster() throws Exception {
    Assume.assumeTrue("Set -D" + StandaloneTestSupport.ENABLE_PROPERTY
            + "=true to run StandaloneClusterMatrixOpsTest end-to-end.",
        StandaloneTestSupport.enabled());

    runDir = StandaloneTestSupport.newRunDir("angel-cluster-matrix-test");
    cluster = StandaloneTestSupport.launchCluster(runDir, PS_NUM, 120_000L);

    attach = newAttachClient();
    attach.startPSServer();
    attach.waitForAllPS(PS_NUM);
    psAgent = StandaloneTestSupport.startPSAgent(engineConf(), cluster.master);
  }

  @AfterClass public static void tearDownCluster() {
    if (psAgent != null) {
      try {
        psAgent.stop();
      } catch (Throwable t) {
        LOG.warn("stop PSAgent failed", t);
      }
    }
    if (attach != null) {
      try {
        attach.stop();
      } catch (Throwable t) {
        LOG.warn("detach failed", t);
      }
    }
    if (cluster != null) {
      cluster.stop();
    }
    StandaloneTestSupport.deleteQuietly(runDir);
  }

  /** Configuration an external engine would carry: just the master address and PS count. */
  private static Configuration engineConf() {
    Configuration conf = new Configuration();
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
    conf.set(AngelAttachClient.ANGEL_MASTER_HOST, cluster.master.getIp());
    conf.setInt(AngelAttachClient.ANGEL_MASTER_PORT, cluster.master.getPort());
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, PS_NUM);
    return conf;
  }

  private static AngelAttachClient newAttachClient() {
    return new AngelAttachClient(engineConf());
  }

  private static void createMatrices(MatrixContext... mats) throws Exception {
    attach.createMatrices(Arrays.asList(mats));
    // Matrices were created by another client; pull the new metadata into the PSAgent.
    psAgent.refreshMatrixInfo();
  }

  private static String localPath(String name) {
    return StandaloneTestSupport.LOCAL_FS + runDir.getAbsolutePath() + "/" + name;
  }

  @Test public void testDenseMatrixComputeThroughAttachedCluster() throws Exception {
    String name = "cl_dense_double";
    int rows = 3;
    int cols = 8000;
    createMatrices(StandaloneMatrixOpsTest.rangeMatrix(name, rows, cols, 1, cols / 4,
        RowType.T_DOUBLE_DENSE));
    MatrixClient client = psAgent.getMatrixClient(name, 0);

    // w <- w + x ; w <- w + 2x  => w = 3x, with x[r][i] = r - i / 1000
    for (int k = 1; k <= 2; k++) {
      for (int r = 0; r < rows; r++) {
        IntDoubleVector x = VFactory.denseDoubleVector(cols);
        for (int i = 0; i < cols; i++) {
          x.set(i, k * (r - i / 1000.0));
        }
        x.setRowId(r);
        client.increment(x);
      }
    }

    for (int r = 0; r < rows; r++) {
      IntDoubleVector row = (IntDoubleVector) client.getRow(r, true);
      for (int i = 0; i < cols; i += 211) {
        assertEquals("row " + r + " col " + i, 3 * (r - i / 1000.0), row.get(i), EPS);
      }
    }

    // A pulled row can be used for local math and pushed back: w0 <- w0 - w1 = 3 * (0 - 1).
    IntDoubleVector w0 = (IntDoubleVector) client.getRow(0, true);
    IntDoubleVector w1 = (IntDoubleVector) client.getRow(1, true);
    IntDoubleVector diff = (IntDoubleVector) w0.sub(w1);
    diff.setRowId(0);
    client.update(diff);
    IntDoubleVector after = (IntDoubleVector) client.get(0, new int[] {0, 1999, 2000, cols - 1});
    for (int idx : new int[] {0, 1999, 2000, cols - 1}) {
      assertEquals(-3.0, after.get(idx), EPS);
    }
  }

  @Test public void testSparseMatricesThroughAttachedCluster() throws Exception {
    String intKey = "cl_sparse_float";
    String longKey = "cl_longkey_float";
    int intCols = 60000;
    long longCols = 1L << 40;
    MatrixContext longKeyMat = new MatrixContext(longKey, 1, longCols, -1, -1, -1,
        RowType.T_FLOAT_SPARSE_LONGKEY);
    longKeyMat.setPartitionerClass(HashPartitioner.class);
    createMatrices(StandaloneMatrixOpsTest.rangeMatrix(intKey, 1, intCols, 1, intCols / 3,
        RowType.T_FLOAT_SPARSE), longKeyMat);

    MatrixClient intClient = psAgent.getMatrixClient(intKey, 0);
    IntFloatVector iv = VFactory.sparseFloatVector(intCols);
    for (int i = 0; i < intCols; i += 1000) {
      iv.set(i, i / 1000.0f);
    }
    iv.setRowId(0);
    intClient.increment(iv);
    IntFloatVector igot = (IntFloatVector) intClient.getRow(0, true);
    assertEquals(intCols / 1000, igot.getSize());
    assertEquals(59.0f, igot.get(59000), EPS);
    assertEquals(0.0f, igot.get(59001), EPS);

    MatrixClient longClient = psAgent.getMatrixClient(longKey, 0);
    long[] keys = new long[64];
    for (int k = 0; k < keys.length; k++) {
      keys[k] = k * (longCols / keys.length) + k;
    }
    LongFloatVector lv = VFactory.sparseLongKeyFloatVector(longCols);
    for (int k = 0; k < keys.length; k++) {
      lv.set(keys[k], 1.0f + k);
    }
    lv.setRowId(0);
    longClient.increment(lv);
    longClient.increment(lv.copy());
    LongFloatVector lgot = (LongFloatVector) longClient.get(0, keys);
    for (int k = 0; k < keys.length; k++) {
      assertEquals("key " + keys[k], 2.0f * (1 + k), lgot.get(keys[k]), EPS);
    }
  }

  /** Save / zero / load driven by the attach client, mirroring a checkpoint-restore cycle. */
  @Test public void testSaveAndLoadThroughAttachClient() throws Exception {
    String name = "cl_save_dense";
    int cols = 4000;
    createMatrices(StandaloneMatrixOpsTest.rangeMatrix(name, 1, cols, 1, cols / 4,
        RowType.T_DOUBLE_DENSE));
    MatrixClient client = psAgent.getMatrixClient(name, 0);

    double[] expected = new double[cols];
    IntDoubleVector v = VFactory.denseDoubleVector(cols);
    for (int i = 0; i < cols; i++) {
      expected[i] = Math.sin(i);
      v.set(i, expected[i]);
    }
    v.setRowId(0);
    client.increment(v);

    String path = localPath("attach-save");
    ModelSaveContext save = new ModelSaveContext(path);
    save.addMatrix(new MatrixSaveContext(name));
    attach.save(save);
    FileSystem fs = new Path(path).getFileSystem(new Configuration());
    assertTrue("model dir must contain the saved matrix",
        fs.exists(new Path(path, name)));

    client.asyncUpdate(new Zero(new Zero.ZeroParam(client.getMatrixId(), false)))
        .get(60, TimeUnit.SECONDS);
    assertEquals(0.0, ((IntDoubleVector) client.getRow(0, true)).sum(), EPS);

    ModelLoadContext load = new ModelLoadContext(path);
    load.addMatrix(new MatrixLoadContext(name));
    attach.load(load);
    IntDoubleVector row = (IntDoubleVector) client.getRow(0, true);
    assertArrayEquals(expected, row.getStorage().getValues(), EPS);
  }

  /**
   * The cluster is long-running: an engine may detach and a new one attach later and still
   * see the parameters left on the PS.
   */
  @Test public void testParametersSurviveReattach() throws Exception {
    String name = "cl_reattach_dense";
    int cols = 1000;
    createMatrices(StandaloneMatrixOpsTest.rangeMatrix(name, 1, cols, 1, cols / 2,
        RowType.T_DOUBLE_DENSE));
    IntDoubleVector v = VFactory.denseDoubleVector(cols);
    for (int i = 0; i < cols; i++) {
      v.set(i, 7.0);
    }
    v.setRowId(0);
    psAgent.getMatrixClient(name, 0).increment(v);

    // Detach the whole "engine" (data plane and control plane) ...
    psAgent.stop();
    psAgent = null;
    attach.stop();
    attach = null;

    // ... and attach a fresh one.
    attach = newAttachClient();
    attach.startPSServer();
    attach.waitForAllPS(PS_NUM);
    psAgent = StandaloneTestSupport.startPSAgent(engineConf(), cluster.master);

    IntDoubleVector row = (IntDoubleVector) psAgent.getMatrixClient(name, 0).getRow(0, true);
    assertEquals(7.0 * cols, row.sum(), EPS);
  }
}
