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

import com.tencent.angel.PartitionKey;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.LongDoubleVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ml.matrix.psf.update.update.IncrementRows;
import com.tencent.angel.ml.matrix.psf.update.update.IncrementRowsParam;
import com.tencent.angel.ml.matrix.psf.update.zero.Zero;
import com.tencent.angel.model.MatrixLoadContext;
import com.tencent.angel.model.MatrixSaveContext;
import com.tencent.angel.model.ModelLoadContext;
import com.tencent.angel.model.ModelSaveContext;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.storage.partitioner.HashPartitioner;
import com.tencent.angel.ps.storage.partitioner.RangePartitioner;
import com.tencent.angel.psagent.PSAgent;
import com.tencent.angel.psagent.matrix.MatrixClient;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Data-plane test for the single-host STANDALONE deploy mode.
 *
 * <p>The master runs in the test JVM, two Parameter Servers run as child JVMs, and a
 * {@link PSAgent} connected to the master (the same client a Spark executor uses) defines
 * matrices and performs basic computation on them: increment, update, row/rows/index get,
 * PSF-based update, concurrent increments, and a save/load round trip.
 *
 * <p>Spawns child JVMs, so it only runs when explicitly enabled:
 * <pre>
 *   mvn -o test -Dangel.test.standalone.enabled=true \
 *       -Dtest=StandaloneMatrixOpsTest -pl angel-ps/core
 * </pre>
 */
public class StandaloneMatrixOpsTest {

  private static final Log LOG = LogFactory.getLog(StandaloneMatrixOpsTest.class);

  private static final int PS_NUM = 2;
  private static final double EPS = 1e-6;

  private static final String DENSE_DOUBLE = "sa_dense_double";
  private static final int DENSE_ROWS = 4;
  private static final int DENSE_COLS = 10000;

  private static final String DENSE_INT = "sa_dense_int";
  private static final int INT_COLS = 1000;

  private static final String SPARSE_FLOAT = "sa_sparse_float";
  private static final int SPARSE_COLS = 100000;

  private static final String LONGKEY_DOUBLE = "sa_longkey_double";
  private static final long LONGKEY_COLS = 1L << 34;

  private static final String PSF_MAT = "sa_psf_dense";
  private static final String CONCURRENT_MAT = "sa_concurrent_dense";
  private static final String SAVE_MAT = "sa_save_dense";

  private static File runDir;
  private static AngelClient owner;
  private static PSAgent psAgent;

  @BeforeClass public static void setUpCluster() throws Exception {
    Assume.assumeTrue("Set -D" + StandaloneTestSupport.ENABLE_PROPERTY
        + "=true to run StandaloneMatrixOpsTest end-to-end.", StandaloneTestSupport.enabled());

    runDir = StandaloneTestSupport.newRunDir("angel-sa-matrix-test");
    Configuration conf = StandaloneTestSupport.psOnlyConf(runDir, PS_NUM);

    owner = AngelClientFactory.get(conf);
    assertTrue(owner instanceof AngelStandaloneClient);

    // Matrices declared up-front by the owner client, like an Angel job driver would.
    List<MatrixContext> mats = new ArrayList<>();
    mats.add(rangeMatrix(DENSE_DOUBLE, DENSE_ROWS, DENSE_COLS, 2, DENSE_COLS / 4,
        RowType.T_DOUBLE_DENSE));
    mats.add(rangeMatrix(DENSE_INT, 1, INT_COLS, 1, INT_COLS / 4, RowType.T_INT_DENSE));
    mats.add(rangeMatrix(SPARSE_FLOAT, 2, SPARSE_COLS, 1, SPARSE_COLS / 4,
        RowType.T_FLOAT_SPARSE));
    MatrixContext longKey = new MatrixContext(LONGKEY_DOUBLE, 1, LONGKEY_COLS, -1, -1, -1,
        RowType.T_DOUBLE_SPARSE_LONGKEY);
    longKey.setPartitionerClass(HashPartitioner.class);
    mats.add(longKey);

    owner.startPSServer();
    owner.waitForAllPS(PS_NUM);
    owner.createMatrices(mats);

    Location master = owner.getMasterLocation();
    LOG.info("standalone master at " + master + ", starting PSAgent");
    psAgent = StandaloneTestSupport.startPSAgent(new Configuration(conf), master);
  }

  @AfterClass public static void tearDownCluster() {
    if (psAgent != null) {
      try {
        psAgent.stop();
      } catch (Throwable t) {
        LOG.warn("stop PSAgent failed", t);
      }
    }
    if (owner != null) {
      try {
        owner.stop();
      } catch (Throwable t) {
        LOG.warn("stop owner client failed", t);
      }
    }
    StandaloneTestSupport.deleteQuietly(runDir);
  }

  static MatrixContext rangeMatrix(String name, int rows, long cols, int rowsInBlock,
      long colsInBlock, RowType type) {
    MatrixContext mc = new MatrixContext(name, rows, cols, -1, rowsInBlock, colsInBlock, type);
    mc.setPartitionerClass(RangePartitioner.class);
    return mc;
  }

  /** The matrices must really be spread over both PS so every op crosses process borders. */
  @Test public void testMatricesArePartitionedAcrossAllPS() {
    for (String name : Arrays.asList(DENSE_DOUBLE, DENSE_INT, SPARSE_FLOAT, LONGKEY_DOUBLE)) {
      int matrixId = psAgent.getMatrixMetaManager().getMatrixId(name);
      List<PartitionKey> parts = psAgent.getMatrixMetaManager().getPartitions(matrixId);
      Set<ParameterServerId> servers = new HashSet<>();
      for (PartitionKey part : parts) {
        servers.add(psAgent.getMatrixMetaManager().getMasterPS(part));
      }
      LOG.info(name + ": " + parts.size() + " partitions on " + servers);
      assertTrue(name + " should have more than one partition", parts.size() > 1);
      assertEquals(name + " should be served by every PS", PS_NUM, servers.size());
    }
  }

  @Test public void testDenseDoubleIncrementAndGet() throws Exception {
    MatrixClient client = psAgent.getMatrixClient(DENSE_DOUBLE, 0);

    // Row r receives (r + 1) * i on column i, twice: PS must accumulate the deltas.
    for (int round = 0; round < 2; round++) {
      for (int r = 0; r < DENSE_ROWS; r++) {
        IntDoubleVector delta = VFactory.denseDoubleVector(DENSE_COLS);
        for (int i = 0; i < DENSE_COLS; i++) {
          delta.set(i, (r + 1) * (double) i);
        }
        delta.setRowId(r);
        client.increment(delta);
      }
    }

    // Whole-row get.
    for (int r = 0; r < DENSE_ROWS; r++) {
      IntDoubleVector row = (IntDoubleVector) client.getRow(r, true);
      assertEquals(DENSE_COLS, row.getDim());
      for (int i = 0; i < DENSE_COLS; i += 97) {
        assertEquals("row " + r + " col " + i, 2.0 * (r + 1) * i, row.get(i), EPS);
      }
      // sum_i 2 (r+1) i = (r+1) n (n-1)
      assertEquals((r + 1) * (double) DENSE_COLS * (DENSE_COLS - 1), row.sum(), EPS);
    }

    // Multi-row get.
    int[] rowIds = {0, 1, 2, 3};
    Vector[] rows = client.getRows(rowIds, true);
    assertEquals(rowIds.length, rows.length);
    for (int k = 0; k < rows.length; k++) {
      IntDoubleVector row = (IntDoubleVector) rows[k];
      assertEquals(rowIds[k], row.getRowId());
      assertEquals(2.0 * (rowIds[k] + 1) * (DENSE_COLS - 1), row.get(DENSE_COLS - 1), EPS);
    }

    // Indexed get spanning every column partition.
    int[] indices = {0, 1, 2499, 2500, 5000, 7499, 7500, DENSE_COLS - 1};
    IntDoubleVector part = (IntDoubleVector) client.get(3, indices);
    for (int idx : indices) {
      assertEquals("index " + idx, 2.0 * 4 * idx, part.get(idx), EPS);
    }
  }

  @Test public void testDenseIntIncrement() throws Exception {
    MatrixClient client = psAgent.getMatrixClient(DENSE_INT, 0);
    int[] values = new int[INT_COLS];
    for (int i = 0; i < INT_COLS; i++) {
      values[i] = i % 7 - 3;
    }
    IntIntVector delta = VFactory.denseIntVector(values);
    delta.setRowId(0);
    client.increment(delta);
    client.increment(delta.copy());

    IntIntVector row = (IntIntVector) client.getRow(0, true);
    for (int i = 0; i < INT_COLS; i++) {
      assertEquals("col " + i, 2 * values[i], row.get(i));
    }
  }

  @Test public void testSparseFloatUpdateOverwrites() throws Exception {
    MatrixClient client = psAgent.getMatrixClient(SPARSE_FLOAT, 0);
    int[] indices = {3, 24999, 25000, 50001, 75000, SPARSE_COLS - 1};

    IntFloatVector first = VFactory.sparseFloatVector(SPARSE_COLS);
    for (int idx : indices) {
      first.set(idx, 100.0f);
    }
    first.setRowId(1);
    client.increment(first);

    // update() must replace, not accumulate.
    IntFloatVector second = VFactory.sparseFloatVector(SPARSE_COLS);
    for (int idx : indices) {
      second.set(idx, idx * 0.5f);
    }
    second.setRowId(1);
    client.update(second);

    IntFloatVector got = (IntFloatVector) client.get(1, new int[] {3, 4, 25000, 50001,
        SPARSE_COLS - 1});
    assertEquals(1.5f, got.get(3), EPS);
    assertEquals("untouched index must stay zero", 0.0f, got.get(4), EPS);
    assertEquals(12500.0f, got.get(25000), EPS);
    assertEquals(25000.5f, got.get(50001), EPS);
    assertEquals((SPARSE_COLS - 1) * 0.5f, got.get(SPARSE_COLS - 1), EPS);

    // Row 0 was never written.
    IntFloatVector row0 = (IntFloatVector) client.getRow(0, true);
    assertEquals(0, row0.getSize());
  }

  @Test public void testLongKeySparseHashPartitioned() throws Exception {
    MatrixClient client = psAgent.getMatrixClient(LONGKEY_DOUBLE, 0);
    long[] keys = {0L, 7L, 1L << 20, (1L << 32) + 5, LONGKEY_COLS - 1};

    LongDoubleVector delta = VFactory.sparseLongKeyDoubleVector(LONGKEY_COLS);
    for (int k = 0; k < keys.length; k++) {
      delta.set(keys[k], k + 1.0);
    }
    delta.setRowId(0);
    client.increment(delta);
    client.increment(delta.copy());

    long[] query = new long[keys.length + 1];
    System.arraycopy(keys, 0, query, 0, keys.length);
    query[keys.length] = 123456789L;
    LongDoubleVector got = (LongDoubleVector) client.get(0, query);
    for (int k = 0; k < keys.length; k++) {
      assertEquals("key " + keys[k], 2.0 * (k + 1), got.get(keys[k]), EPS);
    }
    assertEquals("missing key must read as zero", 0.0, got.get(123456789L), EPS);
  }

  /** Matrix created at runtime through the PSAgent (the Spark-on-Angel driver path). */
  @Test public void testPSFIncrementRowsAndZeroOnRuntimeMatrix() throws Exception {
    int cols = 2000;
    psAgent.createMatrix(rangeMatrix(PSF_MAT, 2, cols, 1, cols / 4, RowType.T_DOUBLE_DENSE),
        30000L);
    MatrixClient client = psAgent.getMatrixClient(PSF_MAT, 0);
    int matrixId = client.getMatrixId();

    Vector[] updates = new Vector[2];
    for (int r = 0; r < 2; r++) {
      IntDoubleVector v = VFactory.denseDoubleVector(cols);
      for (int i = 0; i < cols; i++) {
        v.set(i, r == 0 ? 1.0 : i);
      }
      v.setRowId(r);
      updates[r] = v;
    }
    client.asyncUpdate(new IncrementRows(new IncrementRowsParam(matrixId, updates)))
        .get(60, TimeUnit.SECONDS);

    assertEquals(cols, ((IntDoubleVector) client.getRow(0, true)).sum(), EPS);
    assertEquals(cols * (cols - 1) / 2.0, ((IntDoubleVector) client.getRow(1, true)).sum(),
        EPS);

    client.asyncUpdate(new Zero(new Zero.ZeroParam(matrixId, false))).get(60, TimeUnit.SECONDS);
    for (int r = 0; r < 2; r++) {
      IntDoubleVector row = (IntDoubleVector) client.getRow(r, true);
      assertEquals("row " + r + " must be zero after Zero PSF", 0.0, row.sum(), EPS);
    }
  }

  /** Several tasks push to the same row concurrently; no update may be lost. */
  @Test public void testConcurrentIncrementsFromManyTasks() throws Exception {
    final int cols = 5000;
    final int tasks = 4;
    final int rounds = 25;
    psAgent.createMatrix(rangeMatrix(CONCURRENT_MAT, 1, cols, 1, cols / 4,
        RowType.T_DOUBLE_DENSE), 30000L);

    ExecutorService pool = Executors.newFixedThreadPool(tasks);
    try {
      List<Future<Void>> futures = new ArrayList<>();
      for (int t = 0; t < tasks; t++) {
        final int taskIndex = t;
        futures.add(pool.submit(new Callable<Void>() {
          @Override public Void call() throws Exception {
            MatrixClient client = psAgent.getMatrixClient(CONCURRENT_MAT, taskIndex);
            for (int round = 0; round < rounds; round++) {
              IntDoubleVector delta = VFactory.denseDoubleVector(cols);
              for (int i = 0; i < cols; i++) {
                delta.set(i, 1.0);
              }
              delta.setRowId(0);
              client.increment(delta);
            }
            return null;
          }
        }));
      }
      for (Future<Void> f : futures) {
        f.get(120, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }

    IntDoubleVector row = (IntDoubleVector) psAgent.getMatrixClient(CONCURRENT_MAT, 0)
        .getRow(0, true);
    double expected = tasks * rounds;
    for (int i = 0; i < cols; i += 123) {
      assertEquals("col " + i, expected, row.get(i), EPS);
    }
    assertEquals(expected * cols, row.sum(), EPS);
  }

  /** Save to local FS, wipe the matrix on PS, load it back and compare. */
  @Test public void testSaveZeroAndLoadRoundTrip() throws Exception {
    int cols = 3000;
    owner.createMatrices(Arrays.asList(
        rangeMatrix(SAVE_MAT, 2, cols, 1, cols / 3, RowType.T_DOUBLE_DENSE)));
    psAgent.refreshMatrixInfo();

    MatrixClient client = psAgent.getMatrixClient(SAVE_MAT, 0);
    double[][] expected = new double[2][cols];
    for (int r = 0; r < 2; r++) {
      IntDoubleVector v = VFactory.denseDoubleVector(cols);
      for (int i = 0; i < cols; i++) {
        expected[r][i] = (r + 1) * 0.25 * i;
        v.set(i, expected[r][i]);
      }
      v.setRowId(r);
      client.increment(v);
    }

    String path = StandaloneTestSupport.LOCAL_FS + runDir.getAbsolutePath() + "/model-save";
    ModelSaveContext save = new ModelSaveContext(path);
    save.addMatrix(new MatrixSaveContext(SAVE_MAT));
    owner.save(save);

    client.asyncUpdate(new Zero(new Zero.ZeroParam(client.getMatrixId(), false)))
        .get(60, TimeUnit.SECONDS);
    assertEquals(0.0, ((IntDoubleVector) client.getRow(1, true)).sum(), EPS);

    ModelLoadContext load = new ModelLoadContext(path);
    load.addMatrix(new MatrixLoadContext(SAVE_MAT));
    owner.load(load);

    for (int r = 0; r < 2; r++) {
      IntDoubleVector row = (IntDoubleVector) client.getRow(r, true);
      assertArrayEquals("row " + r + " after load", expected[r], row.getStorage().getValues(),
          EPS);
    }
  }
}
