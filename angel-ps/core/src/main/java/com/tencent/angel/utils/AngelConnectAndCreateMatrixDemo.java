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

package com.tencent.angel.utils;

import com.tencent.angel.client.attach.AngelAttachClient;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.partitioner.RangePartitioner;
import java.util.Arrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Minimal "hello world" against an already-running Angel master + PS cluster.
 *
 * <p>Reads the master address from a single command-line argument
 * ({@code <ip>:<port>}), attaches via {@link AngelAttachClient}, creates two example
 * matrices on the PSes, prints the resulting matrix metas to stdout and detaches. The
 * remote cluster is left untouched (the matrices stay on the PSes for subsequent
 * clients).
 *
 * <p>Run after {@code bin/angel-cluster-up} has printed an
 * {@code ANGEL_MASTER=<ip>:<port>} line:
 * <pre>
 *   java -cp $ANGEL_HOME/lib/* \
 *        com.tencent.angel.utils.AngelConnectAndCreateMatrixDemo \
 *        11.69.175.83:20896
 * </pre>
 *
 * <p>The two matrices created are:
 * <ul>
 *   <li>{@code demo_dense} — 4 rows &times; 100 cols, {@link RowType#T_DOUBLE_DENSE}
 *       with an explicit {@link RangePartitioner} (dense rows are not allowed to use
 *       Angel's default {@code HashPartitioner}).</li>
 *   <li>{@code demo_sparse} — 1 row &times; 1024 cols, {@link RowType#T_FLOAT_SPARSE}.</li>
 * </ul>
 *
 * <p>For a richer demo (custom matrix specs, saving to HDFS, multiple PSes …) see
 * {@link AngelAttachClientDemo}.
 */
public class AngelConnectAndCreateMatrixDemo {

  private static final Log LOG = LogFactory.getLog(AngelConnectAndCreateMatrixDemo.class);

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println(
          "Usage: java " + AngelConnectAndCreateMatrixDemo.class.getName() + " <host>:<port>");
      System.exit(2);
      return;
    }
    Location master = parseHostPort(args[0]);

    // Hadoop Configuration used by AngelAttachClient. The bare minimum is the master
    // location (passed via the constructor below); ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST
    // is set to keep re-runs idempotent if you later wire in save().
    Configuration conf = new Configuration();
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);

    AngelAttachClient client = new AngelAttachClient(conf, master);
    LOG.info("Connecting to Angel master at " + master.getIp() + ":" + master.getPort() + " ...");
    client.startPSServer();
    LOG.info("Attached. appId=" + client.getAppIdString());

    try {
      // Dense matrix: 4 x 100, blocked into 2-row x 50-col tiles. Dense row types must
      // override the default HashPartitioner with RangePartitioner.
      MatrixContext dense = new MatrixContext("demo_dense", 4, 100, -1, -1, -1, 2, 50,
          new java.util.ArrayList<>(), RowType.T_DOUBLE_DENSE);
      dense.setPartitionerClass(RangePartitioner.class);

      // Sparse matrix: 1 x 1024, valid index range = [0, 1024).
      MatrixContext sparse = new MatrixContext("demo_sparse", 1, 1024, 1024, -1, -1, 1, 256,
          new java.util.ArrayList<>(), RowType.T_FLOAT_SPARSE);
      sparse.setPartitionerClass(RangePartitioner.class);

      LOG.info("Creating matrices ...");
      client.createMatrices(Arrays.asList(dense, sparse));

      // The master assigns matrixId / partition layout; print whatever we know locally.
      System.out.println("Created matrices:");
      System.out.println("  " + describe(dense));
      System.out.println("  " + describe(sparse));
      LOG.info("Done. Matrices are now live on the PSes.");
    } finally {
      // Detach only — never stops the remote cluster.
      LOG.info("Detaching ...");
      try {
        client.stop();
      } catch (Throwable t) {
        LOG.warn("detach failed (best-effort)", t);
      }
    }
  }

  private static Location parseHostPort(String hp) {
    int idx = hp.lastIndexOf(':');
    if (idx <= 0 || idx == hp.length() - 1) {
      throw new IllegalArgumentException(
          "master address must look like 'host:port', got: " + hp);
    }
    return new Location(hp.substring(0, idx), Integer.parseInt(hp.substring(idx + 1)));
  }

  private static String describe(MatrixContext mc) {
    return "name=" + mc.getName()
        + " rows=" + mc.getRowNum()
        + " cols=" + mc.getColNum()
        + " rowType=" + mc.getRowType()
        + " rowsPerBlock=" + mc.getMaxRowNumInBlock()
        + " colsPerBlock=" + mc.getMaxColNumInBlock();
  }
}
