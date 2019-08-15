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


package com.tencent.angel.ps.io.save;

import com.google.protobuf.ServiceException;
import com.tencent.angel.RunningMode;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.model.PSMatricesSaveContext;
import com.tencent.angel.model.PSMatrixSaveContext;
import com.tencent.angel.model.output.format.ModelFilesConstent;
import com.tencent.angel.model.output.format.SnapshotFormat;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.client.MasterClient;
import com.tencent.angel.ps.server.data.ServerState;
import com.tencent.angel.utils.HdfsUtil;
import com.tencent.angel.utils.Sort;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Snapshot dumper, it dump the matrices to hdfs once in a while
 */
public class SnapshotDumper {

  private static final Log LOG = LogFactory.getLog(SnapshotDumper.class);
  /**
   * PS context
   */
  private final PSContext context;

  /**
   * Dump interval in milliseconds
   */
  private final int backupIntervalMs;

  /**
   * Dump dispatcher
   */
  private volatile Thread dumpDispatcher;
  private final AtomicBoolean stopped;

  /**
   * Application tmp output path
   */
  private final String outputDir;

  /**
   * Need dump matrices
   */
  private final String[] needDumpMatrices;

  /**
   * Snapshot base directory path
   */
  private final Path baseDirPath;

  /**
   * Last dump iteration number
   */
  private int lastIteration;

  /**
   * Application running mode
   */
  private final RunningMode mode;

  private final int dumpParallel;

  /**
   * Create a SnapshotDumper
   *
   * @param context PS context
   */
  public SnapshotDumper(PSContext context) {
    this.context = context;
    stopped = new AtomicBoolean(false);
    lastIteration = 0;
    mode = context.getRunningMode();

    backupIntervalMs = context.getConf()
        .getInt(AngelConf.ANGEL_PS_BACKUP_INTERVAL_MS,
            AngelConf.DEFAULT_ANGEL_PS_BACKUP_INTERVAL_MS);

    dumpParallel = context.getConf()
        .getInt(AngelConf.ANGEL_PS_BACKUP_PARALLEL, AngelConf.DEFAULT_ANGEL_PS_BACKUP_PARALLEL);

    outputDir = context.getConf().get(AngelConf.ANGEL_JOB_TMP_OUTPUT_PATH);
    baseDirPath = new Path(
        outputDir + Path.SEPARATOR + ModelFilesConstent.snapshotDirName + Path.SEPARATOR + context
            .getPSAttemptId().getPsId());

    String matricesStr = context.getConf().get(AngelConf.ANGEL_PS_BACKUP_MATRICES);
    if (matricesStr == null) {
      needDumpMatrices = null;
    } else {
      needDumpMatrices = matricesStr.split(",");
    }
  }

  /**
   * Init
   */
  public void init() {
  }

  /**
   * Start dump dispatcher
   */
  public void start() {
    LOG.info("Start snapshot dumper");
    dumpDispatcher = new Thread(() -> {
      while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
        try {
          Thread.sleep(backupIntervalMs);
          try {
            LOG.info("to writeSnapshots");
            while (context.getRunningContext().getState() == ServerState.BUSY) {
              Thread.sleep(5000);
            }
            writeSnapshots();
          } catch (Exception ioe) {
            LOG.error("write snapshots error: ", ioe);
          }
        } catch (InterruptedException e) {
          if (!stopped.get()) {
            LOG.warn("Snapshot dump dispatcher is interrupted. Returning.");
          }
        }
      }
    });
    dumpDispatcher.setName("snapshot-dump-dispatcher");
    dumpDispatcher.start();
  }

  /**
   * Write snapshot
   */
  private void writeSnapshots() throws Exception {
    List<Integer> matrixIds = null;
    if (needDumpMatrices == null) {
      matrixIds = new ArrayList<>(context.getMatrixMetaManager().getMatrixMetas().keySet());
    } else {
      matrixIds = toMatrixIds();
    }

    if (matrixIds.isEmpty()) {
      return;
    }

    List<Integer> needDumpMatrices = filter(matrixIds);
    if (needDumpMatrices != null && !needDumpMatrices.isEmpty()) {
      checkpoint(matrixIds, 0, true, false);
    }
  }

  /**
   * Get the matrices that need dump
   *
   * @param matrixIds all matrices
   * @return the matrices that need dump
   */
  private List<Integer> filter(List<Integer> matrixIds) throws ServiceException {
    int size = matrixIds.size();
    List<Integer> ret = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      if (checkNeedDump(matrixIds.get(i))) {
        ret.add(matrixIds.get(i));
      }
    }

    return ret;
  }

  /**
   * Is this matrix need dump
   *
   * @param matrixId matrix id
   * @return true mean need dump
   */
  private boolean checkNeedDump(int matrixId) throws ServiceException {
    if (mode == RunningMode.ANGEL_PS) {
      return true;
    } else {
      MasterClient master = context.getMaster();
      if (master == null) {
        return false;
      } else {
        int iteration = context.getMaster().getIteration();
        return iteration > lastIteration;
      }
    }
  }

  private List<Integer> toMatrixIds() {
    List<Integer> matrixIds = new ArrayList<>(needDumpMatrices.length);
    for (int i = 0; i < needDumpMatrices.length; i++) {
      MatrixMeta meta = context.getMatrixMetaManager().getMatrixMeta(needDumpMatrices[i]);
      if (meta != null) {
        matrixIds.add(meta.getId());
      }
    }

    return matrixIds;
  }

  public void stop() {
    if (!stopped.getAndSet(true)) {
      if (dumpDispatcher != null) {
        dumpDispatcher.interrupt();
      }
    }
  }

  private void checkpoint(List<Integer> matrixIds, int checkpointId, boolean cloneFirst,
      boolean sortFirst) throws IOException {
    if (matrixIds == null || matrixIds.isEmpty()) {
      return;
    }

    List<PSMatrixSaveContext> saveContexts = new ArrayList<>(matrixIds.size());
    List<Path> checkpointItemPaths = new ArrayList<>(matrixIds.size());
    List<Path> tempPaths = new ArrayList<>(matrixIds.size());

    for (int matrixId : matrixIds) {
      Path checkpointItemPath = genCheckpointPath(matrixId, checkpointId);
      Path tempPath = genTmpCheckpointPath(checkpointItemPath);
      checkpointItemPaths.add(checkpointItemPath);
      tempPaths.add(tempPath);

      MatrixMeta meta = context.getMatrixMetaManager().getMatrixMeta(matrixId);
      saveContexts.add(
          new PSMatrixSaveContext(matrixId,
              new ArrayList<>(meta.getPartitionMetas().keySet()),
              null, SnapshotFormat.class.getName(),
              tempPath.toString(),
              cloneFirst, sortFirst));
    }

    context.getIOExecutors()
        .save(new PSMatricesSaveContext(-1, -1, saveContexts), dumpParallel);

    // Rename temp to item path
    FileSystem fs = baseDirPath.getFileSystem(context.getConf());
    for (int i = 0; i < matrixIds.size(); i++) {
      HdfsUtil.rename(tempPaths.get(i), checkpointItemPaths.get(i), fs);
      clearOldCheckpoint(fs, genMatrixPath(matrixIds.get(0)));
    }
  }

  private Path genMatrixPath(int matrixId) throws IOException {
    String matrixName = context.getMatrixMetaManager().getMatrixMeta(matrixId).getName();
    LOG.info("checkpoint matrix " + matrixName);

    // Checkpoint base path = Base dir/matrix name
    return new Path(baseDirPath, matrixName);
  }

  private Path genCheckpointPath(int matrixId, int checkpointId) throws IOException {
    // Path for this checkpoint
    Path checkpointItemPath = new Path(genMatrixPath(matrixId), "" + checkpointId);
    return checkpointItemPath;
  }

  private Path genTmpCheckpointPath(Path checkpointItemPath) throws IOException {
    // Generate tmp path
    Path tmpPath = HdfsUtil.toTmpPath(checkpointItemPath);

    LOG.info("Checkpoint item path=" + checkpointItemPath.toString());
    LOG.info("Checkpoint tmp path=" + tmpPath.toString());

    FileSystem fs = checkpointItemPath.getFileSystem(context.getConf());
    if (fs.exists(tmpPath)) {
      boolean ret = fs.delete(tmpPath, true);
      if (!ret) {
        LOG.error("Delete tmp dir " + tmpPath.toString() + " failed");
        throw new IOException("Delete tmp dir " + tmpPath.toString() + " failed ");
      }
    }

    return tmpPath;
  }

  public void checkpoint(int matrixId, int checkpointId) throws IOException {
    List<Integer> matrixIds = new ArrayList<>(1);
    matrixIds.add(matrixId);
    checkpoint(matrixIds, checkpointId, false, false);
  }

  private void clearOldCheckpoint(FileSystem fs, Path checkpointPath) throws IOException {
    int maxCheckpoint = context.getConf().getInt(AngelConf.ANGEL_PS_CHECKPOINTS_MAX_NUM,
        AngelConf.DEFAULT_ANGEL_PS_CHECKPOINTS_MAX_NUM);
    FileStatus[] status = fs.listStatus(checkpointPath);
    if (status.length <= maxCheckpoint) {
      return;
    }

    // Delete old checkpoints
    int[] checkpointIds = new int[status.length];
    for (int i = 0; i < status.length; i++) {
      try {
        checkpointIds[i] = Integer.valueOf(status[i].getPath().getName());
      } catch (Throwable x) {
        LOG.warn("Path " + status[i].getPath().toString()
            + " is not a valid checkpoint path, just remove it");
      }
    }

    Sort.quickSort(checkpointIds, status, 0, checkpointIds.length - 1);
    for (int i = 0; i < checkpointIds.length - maxCheckpoint; i++) {
      try {
        if (!fs.delete(status[i].getPath(), true)) {
          LOG.warn("Delete path " + status[i].getPath() + " failed ");
        } else {
          LOG.info("Delete old checkpoint " + status[i].getPath() + " failed ");
        }
      } catch (Throwable x) {
        LOG.warn("Delete path " + status[i].getPath() + " failed ", x);
      }
    }
  }
}
