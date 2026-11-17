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
import com.tencent.angel.model.MatrixSaveContext;
import com.tencent.angel.model.ModelSaveContext;
import com.tencent.angel.ps.storage.partitioner.RangePartitioner;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Standalone demo program that <b>attaches</b> to an already-running Angel master+PS
 * cluster (e.g. one launched by {@link AngelPSClusterMain} / {@code bin/angel-cluster-up})
 * and exercises the most common control-plane APIs through {@link AngelAttachClient}:
 *
 * <ol>
 *   <li>Connect to the external master via RPC and start a heartbeat client.</li>
 *   <li>Create one or more matrices on the running PSes.</li>
 *   <li>Save those matrices to a user-specified output path.</li>
 *   <li>Detach (does NOT stop the remote cluster).</li>
 * </ol>
 *
 * <h3>How to run</h3>
 * <pre>
 *   # 1. bring up a long-running Angel PS cluster on hosts host-a/host-b
 *   bin/angel-cluster-up host-a,host-b
 *   # ...prints "ANGEL_MASTER=host-a:36004"
 *
 *   # 2. attach to it from this demo
 *   java -cp angel-ps-core.jar:.../* \
 *        com.tencent.angel.utils.AngelAttachClientDemo \
 *        --master host-a:36004 \
 *        --save-path file:///tmp/angel-attach-demo/out \
 *        --matrix demo_w:10:1000 \
 *        --matrix demo_emb:1:1000000:T_FLOAT_SPARSE
 * </pre>
 *
 * <p>{@code --matrix} accepts {@code name:rowNum:colNum[:rowType]}; {@code rowType}
 * defaults to {@code T_DOUBLE_DENSE}. Pass it multiple times to create multiple matrices.
 *
 * <p>The remote cluster is not affected by failures of this program: matrices created
 * here remain on the PSes (they can be reused / saved again by re-running this demo
 * pointing at the same master), and detaching only tears down this JVM's RPC state.
 */
public class AngelAttachClientDemo {

  private static final Log LOG = LogFactory.getLog(AngelAttachClientDemo.class);

  public static void main(String[] args) throws Exception {
    Args parsed = Args.parse(args);
    if (parsed == null) {
      printUsage();
      System.exit(2);
      return;
    }

    Configuration conf = new Configuration();
    // Keep configuration minimal: AngelAttachClient only needs the master location and
    // (optionally) ANGEL_PS_NUMBER for waitForAllPS.
    if (parsed.psNum > 0) {
      conf.setInt(AngelConf.ANGEL_PS_NUMBER, parsed.psNum);
    }
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);

    AngelAttachClient client = new AngelAttachClient(conf, parsed.master);
    LOG.info("Connecting to external Angel master at " + parsed.master.getIp() + ":"
        + parsed.master.getPort() + " ...");
    client.startPSServer();
    LOG.info("Attached. appId=" + client.getAppIdString());

    if (parsed.psNum > 0) {
      LOG.info("Waiting for " + parsed.psNum + " PS(es) to be ready ...");
      client.waitForAllPS(parsed.psNum);
      LOG.info("All PSes are ready.");
    }

    try {
      // 1) Create matrices.
      List<MatrixContext> matrices = parsed.matrices;
      if (matrices.isEmpty()) {
        // Provide a sane default so a no-flag invocation still does something useful.
        MatrixContext def = new MatrixContext("demo_dense", 10, 1000, -1, -1, -1, 5, 200,
            new java.util.ArrayList<>(), RowType.T_DOUBLE_DENSE);
        // Dense matrices require RangePartitioner; the default is HashPartitioner.
        def.setPartitionerClass(RangePartitioner.class);
        matrices = Collections.singletonList(def);
      }
      LOG.info("Creating " + matrices.size() + " matrix/matrices on the remote cluster ...");
      client.createMatrices(matrices);
      for (MatrixContext mc : matrices) {
        LOG.info("  created matrix name=" + mc.getName() + " rows=" + mc.getRowNum()
            + " cols=" + mc.getColNum() + " rowType=" + mc.getRowType());
      }

      // 2) Save the matrices we just created.
      if (parsed.savePath != null) {
        ModelSaveContext save = new ModelSaveContext();
        save.setSavePath(parsed.savePath);
        for (MatrixContext mc : matrices) {
          save.addMatrix(new MatrixSaveContext(mc.getName()));
        }
        LOG.info("Saving " + matrices.size() + " matrix/matrices to " + parsed.savePath
            + " (this blocks until the master reports SUCCESS) ...");
        client.save(save);
        LOG.info("Save finished.");
      } else {
        LOG.info("--save-path not provided, skipping save.");
      }
    } finally {
      // Detach only: this never stops the remote cluster, see AngelAttachClient.stop.
      LOG.info("Detaching from external Angel master ...");
      try {
        client.stop();
      } catch (Throwable t) {
        LOG.warn("detach failed (best-effort)", t);
      }
    }
    LOG.info("AngelAttachClientDemo done.");
  }

  // -------------------------------------------------------------------------------------------

  private static void printUsage() {
    System.err.println(
        "Usage: java com.tencent.angel.utils.AngelAttachClientDemo \\\n"
            + "    --master <host:port> \\\n"
            + "    [--save-path <hdfs-or-file-uri>] \\\n"
            + "    [--ps-num <int>] \\\n"
            + "    [--matrix <name:rows:cols[:rowType]>] (repeatable)\n"
            + "\n"
            + "  rowType defaults to T_DOUBLE_DENSE. Examples:\n"
            + "    --matrix w:10:1000\n"
            + "    --matrix emb:1:1000000:T_FLOAT_SPARSE\n");
  }

  /** Minimal argument parser; intentionally dependency-free. */
  static final class Args {
    Location master;
    String savePath;
    int psNum;
    java.util.List<MatrixContext> matrices = new java.util.ArrayList<>();

    static Args parse(String[] argv) {
      Args a = new Args();
      try {
        for (int i = 0; i < argv.length; i++) {
          String k = argv[i];
          switch (k) {
            case "--master":
              a.master = parseHostPort(argv[++i]);
              break;
            case "--save-path":
              a.savePath = argv[++i];
              break;
            case "--ps-num":
              a.psNum = Integer.parseInt(argv[++i]);
              break;
            case "--matrix":
              a.matrices.add(parseMatrix(argv[++i]));
              break;
            case "-h":
            case "--help":
              return null;
            default:
              System.err.println("Unknown argument: " + k);
              return null;
          }
        }
      } catch (Exception e) {
        System.err.println("Failed to parse arguments: " + e.getMessage());
        return null;
      }
      if (a.master == null) {
        System.err.println("--master is required.");
        return null;
      }
      return a;
    }

    private static Location parseHostPort(String hp) {
      int idx = hp.lastIndexOf(':');
      if (idx <= 0 || idx == hp.length() - 1) {
        throw new IllegalArgumentException("--master must look like host:port, got: " + hp);
      }
      return new Location(hp.substring(0, idx), Integer.parseInt(hp.substring(idx + 1)));
    }

    private static MatrixContext parseMatrix(String spec) {
      String[] parts = spec.split(":");
      if (parts.length < 3) {
        throw new IllegalArgumentException(
            "--matrix expects name:rows:cols[:rowType], got: " + spec);
      }
      String name = parts[0];
      int rows = Integer.parseInt(parts[1]);
      long cols = Long.parseLong(parts[2]);
      RowType rt = parts.length >= 4 ? RowType.valueOf(parts[3]) : RowType.T_DOUBLE_DENSE;
      // Pick reasonable block sizes so the master can partition the matrix without the
      // caller having to think about it.
      int rowsInBlock = Math.max(1, Math.min(rows, 5));
      long colsInBlock = Math.max(1L, Math.min(cols, 1024L));
      MatrixContext mc = new MatrixContext(name, rows, cols, -1, -1, -1, rowsInBlock, colsInBlock,
          new java.util.ArrayList<>(), rt);
      // MatrixContext defaults to HashPartitioner, which Angel rejects for dense row types.
      // Range-partition any dense matrix; sparse ones keep the default.
      if (isDense(rt)) {
        mc.setPartitionerClass(RangePartitioner.class);
      }
      return mc;
    }

    private static boolean isDense(RowType rt) {
      return rt == RowType.T_DOUBLE_DENSE || rt == RowType.T_FLOAT_DENSE
          || rt == RowType.T_INT_DENSE || rt == RowType.T_LONG_DENSE;
    }

    @Override public String toString() {
      return "Args{master=" + master + ", savePath=" + savePath + ", psNum=" + psNum
          + ", matrices=" + Arrays.toString(matrices.toArray()) + "}";
    }
  }
}
