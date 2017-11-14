/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ps.impl;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.tencent.angel.utils.HdfsUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Time;

import com.google.protobuf.ServiceException;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos.MatrixClock;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.GetTaskMatrixClockRequest;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.GetTaskMatrixClockResponse;
import com.tencent.angel.protobuf.generated.PSMasterServiceProtos.TaskMatrixClock;
import com.tencent.angel.ps.PSAttemptId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class SnapshotManager {
  private static final Log LOG = LogFactory.getLog(SnapshotManager.class);
  private final PSAttemptId attemptId;
  private FileSystem fs;
  private Path snapShotBasePath;
  private Thread taskSnapshotsThread;
  private final AtomicBoolean stopped;
  private volatile boolean snapshotLoaded;

  private static String snapshots = "snapshots";
  private static String tmp = "_temporary";
  private static String tempSnapshotFileName = "snapshots_";
  private static String snapshotFileName = "snapshots";
  private static int snapshotFileIndex = 0;

  public SnapshotManager(PSAttemptId attemptId) {
    this.attemptId = attemptId;
    this.stopped = new AtomicBoolean(false);
    this.snapshotLoaded = false;;
  }

  public void init() throws IOException {
    Configuration conf = PSContext.get().getConf();
    String outputPath = conf.get(AngelConf.ANGEL_JOB_TMP_OUTPUT_PATH);
    LOG.info("tmp output dir=" + outputPath);
    if (outputPath == null) {
      throw new IOException("can not find output path setting");
    }

    fs = new Path(outputPath).getFileSystem(conf);
    snapShotBasePath = createSnapshotBaseDir(fs, outputPath);
  }

  public void start() {
    // TODO
    // when we should write snapshot to hdfs? clearly, we have two methods:
    // 1. write snapshot at regular time, if there are updates, just write them.
    // 2. write snapshot every N iterations, this method depends on notification of master
    Configuration conf = PSContext.get().getConf();
    final int backupInterval =
        conf.getInt(AngelConf.ANGEL_PS_BACKUP_INTERVAL_MS,
            AngelConf.DEFAULT_ANGEL_PS_BACKUP_INTERVAL_MS);
    LOG.info("Starting TakeSnapshotsThread, backup interval is " + backupInterval + " ms");
    taskSnapshotsThread = new Thread(new Runnable() {
      @Override
      public void run() {
        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            if (LOG.isDebugEnabled()) {
              LOG.debug("TakeSnapshotsThread is to sleep");
            }
            Thread.sleep(backupInterval);
            try {
              LOG.info("to writeSnapshots");
              writeSnapshots();
            } catch (Exception ioe) {
              LOG.error("Taking snapshots error: ", ioe);
              return;
            }
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.warn("TakeSnapShotsThread interrupted. Returning.");
            }
          }
        }
      }
    });
    taskSnapshotsThread.setName("taskSnapshotsThread");
    taskSnapshotsThread.start();
  }

  public void stop() {
    if (!stopped.getAndSet(true)) {
      if (taskSnapshotsThread != null) {
        taskSnapshotsThread.interrupt();
        try {
          taskSnapshotsThread.join();
        } catch (InterruptedException ie) {
          LOG.warn("InterruptedException while stopping taskSnapshotsThread.");
        }
      }
      taskSnapshotsThread = null;
    }
  }

  private Path createSnapshotBaseDir(FileSystem fs, String outputPath) throws IOException {
    String baseDirStr =
        outputPath + Path.SEPARATOR + snapshots + Path.SEPARATOR
            + String.valueOf(attemptId.getParameterServerId().getIndex()) + Path.SEPARATOR
            + String.valueOf(attemptId.getIndex());

    Path basePath = new Path(baseDirStr);
    LOG.info("create snapshot base directory:" + baseDirStr);

    if (!fs.exists(basePath)) {
      fs.mkdirs(basePath);
    }
    return basePath;
  }

  /**
   * Write snapshots of matrices for recovery.
   *
   * @throws IOException the io exception
   */
  private void writeSnapshots() throws IOException {
    LOG.info("start to write matrix snapshot");
    long startTime = Time.monotonicNow();
    Path snapshotsTempDirPath = getPSSnapshotsTempDir();
    Path snapshotsTempFilePath = new Path(snapshotsTempDirPath, tempSnapshotFileName);
    // FSDataOutputStream output = fileContext.create(snapshotsTempFilePath,
    // EnumSet.of(CreateFlag.CREATE));
    FSDataOutputStream output = fs.create(snapshotsTempFilePath);
    LOG.info("write matrix snapshot to " + snapshotsTempFilePath);
    PSContext.get().getMatrixPartitionManager().writeSnapshot(output);
    output.flush();
    output.close();
    LOG.info("write matrix snapshot over");

    Path snapshotsDestFilePath = getPSSnapshotDestFile();
    HdfsUtil.rename(snapshotsTempFilePath, snapshotsDestFilePath, fs);
    LOG.info("rename " + snapshotsTempFilePath + " to " + snapshotsDestFilePath + " success");
    Path oldSnapshotFile = getOldSnapshotDestFile();
    if (oldSnapshotFile != null) {
      LOG.info("deleting old snapshotFile: " + oldSnapshotFile);
      fs.delete(oldSnapshotFile, false);
    }
    LOG.info("write snapshots cost " + (Time.monotonicNow() - startTime) + "ms!");
  }


  /*
   * @brief get next filename for snapshot
   */
  private Path getPSSnapshotDestFile() throws IOException {
    return new Path(snapShotBasePath, snapshotFileName + "_" + (snapshotFileIndex++));
  }

  // @brief get filename of the old snapshot written before
  private Path getOldSnapshotDestFile() {
    if (snapshotFileIndex <= 1) {
      // no snapshotFile write before, maybe write snapshots the first time
      return null;
    }
    return new Path(snapShotBasePath, snapshotFileName + "_" + (snapshotFileIndex - 2));
  }

  private Path getPSSnapshotsTempDir() throws IOException {
    Path tempSnapshotDir = new Path(snapShotBasePath, tmp);
    if (!fs.exists(tempSnapshotDir)) {
      fs.mkdirs(tempSnapshotDir);
    }
    return tempSnapshotDir;
  }

  private Path getLastSnapshotsFile(Path lastAttemptSnapshotPath) throws IOException {
    Path snapshotsFile = null;
    FileStatus[] allFileStatus = fs.listStatus(lastAttemptSnapshotPath);
    for (FileStatus fileStatus : allFileStatus) {
      if (fileStatus.isFile()) {
        if (snapshotsFile == null) {
          snapshotsFile = fileStatus.getPath();
        } else {
          if (fileStatus.getPath().getName().compareTo(snapshotsFile.getName()) > 0) {
            LOG.info("old snapshotsFile is: " + snapshotsFile + ", new snapshotsFile is: "
                + fileStatus.getPath());
            snapshotsFile = fileStatus.getPath();
          }
        }
      }
    }
    return snapshotsFile;
  }

  private Path getPreviousPSSnapshotsPath() throws IOException {
    Path lastAttemptSnapshotPath = null;
    Path lastAttemptSnapshotDir = null;
    int lastAttempt = attemptId.getIndex();
    while (lastAttempt >= 0) {
      lastAttemptSnapshotDir = new Path(snapShotBasePath.getParent(), String.valueOf(lastAttempt));
      if (fs.exists(lastAttemptSnapshotDir)) {
        lastAttemptSnapshotPath = getLastSnapshotsFile(lastAttemptSnapshotDir);
        if (lastAttemptSnapshotPath == null) {
          lastAttempt--;
          LOG.warn("no snapshotFile in " + lastAttemptSnapshotDir);
          continue;
        }
        break;
      } else {
        LOG.warn("ps: " + attemptId.getParameterServerId() + ", attempt " + lastAttempt
            + " failed without write snapshots!");
        lastAttemptSnapshotPath = null;
        lastAttempt--;
      }
    }
    return lastAttemptSnapshotPath;
  }

  public void processRecovery() {
    if (!snapshotLoaded) {
      try {
        recoveryFromPreviousSnapshorts();
        GetTaskMatrixClockResponse taskMatrixClocks = null;
        try {
          taskMatrixClocks = getTaskMatrixClocks();
          LOG.debug("taskMatrixClocks=" + taskMatrixClocks);
          adjustMatrixClocks(taskMatrixClocks);
        } catch (ServiceException e) {
          LOG.error("get task clocks from master failed.", e);
        }
        snapshotLoaded = true;
      } catch (Exception e) {
        LOG.info("Recovery failed, e", e);
      }
    }
  }

  private void recoveryFromPreviousSnapshorts() throws IOException {
    Path snapshots = getPreviousPSSnapshotsPath();
    if (snapshots != null) {
      LOG.info("ps is recovering from hdfs Snapshot. filePath: " + snapshots);
      long startTs = System.currentTimeMillis();
      FSDataInputStream input = fs.open(snapshots, 65536);
      try{
        PSContext.get().getMatrixPartitionManager().readSnapshot(input);
        LOG.info("read snapshot use time=" + (System.currentTimeMillis() - startTs) + " ms ");
      } finally {
        input.close();
      }
    } else {
      LOG.warn("snapshot file not found, no recovery happened!");
    }
  }

  private GetTaskMatrixClockResponse getTaskMatrixClocks() throws ServiceException {
    return PSContext
        .get()
        .getMaster()
        .getTaskMatrixClocks(
            null,
            GetTaskMatrixClockRequest.newBuilder()
                .setPsAttemptId(ProtobufUtil.convertToIdProto(attemptId)).build());
  }

  private void adjustMatrixClocks(GetTaskMatrixClockResponse taskMatrixClocks) {
    List<TaskMatrixClock> taskClocks = taskMatrixClocks.getTaskMatrixClocksList();
    int taskNum = taskClocks.size();
    TaskMatrixClock taskMatrixClock = null;
    List<MatrixClock> matrixClocks = null;
    int matrixNum = 0;
    for (int i = 0; i < taskNum; i++) {
      taskMatrixClock = taskClocks.get(i);
      matrixClocks = taskMatrixClock.getMatrixClocksList();
      matrixNum = matrixClocks.size();
      for (int j = 0; j < matrixNum; j++) {
        LOG.info("task " + taskMatrixClock.getTaskId().getTaskIndex() + "matrix "
            + matrixClocks.get(j).getMatrixId() + " clock is " + matrixClocks.get(j).getClock());
        PSContext
            .get()
            .getMatrixPartitionManager()
            .setClock(matrixClocks.get(j).getMatrixId(),
                taskMatrixClock.getTaskId().getTaskIndex(), matrixClocks.get(j).getClock());
      }
    }
  }
}
