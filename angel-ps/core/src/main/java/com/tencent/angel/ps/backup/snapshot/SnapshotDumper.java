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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ps.backup.snapshot;

import com.google.protobuf.ServiceException;
import com.tencent.angel.RunningMode;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.transport.ServerState;
import com.tencent.angel.model.output.format.ModelFilesConstent;
import com.tencent.angel.ps.client.MasterClient;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.utils.HdfsUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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
  private final String [] needDumpMatrices;

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

  /**
   * Create a SnapshotDumper
   * @param context PS context
   */
  public SnapshotDumper(PSContext context) {
    this.context = context;
    stopped = new AtomicBoolean(false);
    lastIteration = 0;
    mode = context.getRunningMode();

    backupIntervalMs = context.getConf().getInt(
      AngelConf.ANGEL_PS_BACKUP_INTERVAL_MS,
      AngelConf.DEFAULT_ANGEL_PS_BACKUP_INTERVAL_MS);

    outputDir = context.getConf().get(AngelConf.ANGEL_JOB_TMP_OUTPUT_PATH);
    baseDirPath = new Path(outputDir + Path.SEPARATOR
      + ModelFilesConstent.snapshotDirName + Path.SEPARATOR
      + context.getPSAttemptId().getPsId() + Path.SEPARATOR
      + String.valueOf(context.getPSAttemptId().getIndex()));

    String matricesStr = context.getConf().get(AngelConf.ANGEL_PS_BACKUP_MATRICES);
    if(matricesStr == null) {
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
            while(context.getRunningContext().getState() == ServerState.BUSY) {
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
   * @throws Exception
   */
  private void writeSnapshots() throws Exception {
    List<Integer> matrixIds = null;
    if(needDumpMatrices == null) {
      matrixIds = new ArrayList<>(context.getMatrixMetaManager().getMatrixMetas().keySet());
    } else {
      matrixIds = toMatrixIds();
    }

    if(matrixIds.isEmpty()) {
      return;
    }

    List<Integer> needDumpMatrices = filter(matrixIds);
    if(needDumpMatrices != null && !needDumpMatrices.isEmpty()) {
      FileSystem fs = baseDirPath.getFileSystem(context.getConf());
      Path tmpPath = HdfsUtil.toTmpPath(baseDirPath);
      if(fs.exists(tmpPath)) {
        fs.delete(tmpPath, true);
      }
      context.getMatrixStorageManager().save(filter(matrixIds), tmpPath);
      HdfsUtil.rename(tmpPath, baseDirPath, fs);
    }
  }

  /**
   * Get the matrices that need dump
   * @param matrixIds all matrices
   * @return the matrices that need dump
   * @throws ServiceException
   */
  private List<Integer> filter(List<Integer> matrixIds) throws ServiceException {
    int size = matrixIds.size();
    List<Integer> ret = new ArrayList<>(size);
    for(int i = 0; i < size; i++) {
      if(checkNeedDump(matrixIds.get(i))) {
        ret.add(matrixIds.get(i));
      }
    }

    return ret;
  }

  /**
   * Is this matrix need dump
   * @param matrixId matrix id
   * @return true mean need dump
   * @throws ServiceException
   */
  private boolean checkNeedDump(int matrixId) throws ServiceException {
    if(mode == RunningMode.ANGEL_PS) {
      return true;
    } else {
      MasterClient master = context.getMaster();
      if(master == null) {
        return false;
      } else {
        int iteration = context.getMaster().getIteration();
        return iteration > lastIteration;
      }
    }
  }

  private List<Integer> toMatrixIds() {
    List<Integer> matrixIds = new ArrayList<>(needDumpMatrices.length);
    for(int i = 0; i < needDumpMatrices.length; i++) {
      MatrixMeta meta = context.getMatrixMetaManager().getMatrixMeta(needDumpMatrices[i]);
      if(meta != null) {
        matrixIds.add(meta.getId());
      }
    }

    return matrixIds;
  }

  public void stop() {
    if(!stopped.getAndSet(true)) {
      if(dumpDispatcher != null) {
        dumpDispatcher.interrupt();
      }
    }
  }
}
