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

package com.tencent.angel.ps.recovery.snapshot;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.model.output.format.ModelFilesConstent;
import com.tencent.angel.ps.impl.PSContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Snapshot recover
 */
public class SnapshotRecover {
  private static final Log LOG = LogFactory.getLog(SnapshotRecover.class);
  /**
   * Snapshot base directory
   */
  private final Path baseDirPath;

  /**
   * PS context
   */
  private final PSContext context;

  /**
   * Create SnapshotRecover
   * @param context PS context
   */
  public SnapshotRecover(PSContext context) {
    this.context = context;
    String outputDir = context.getConf().get(AngelConf.ANGEL_JOB_TMP_OUTPUT_PATH);
    baseDirPath = new Path(outputDir + Path.SEPARATOR
      + ModelFilesConstent.snapshotDirName + Path.SEPARATOR
      + context.getPSAttemptId().getPsId());
  }

  /**
   * Get the last written snapshot directory
   * @return the last written snapshot directory
   * @throws IOException
   */
  public Path getSnapshotBasePath() throws IOException {
    FileSystem fs = baseDirPath.getFileSystem(context.getConf());
    Path lastAttemptSnapshotPath = null;
    int lastAttempt = context.getPSAttemptId().getIndex();
    while (lastAttempt >= 0) {
      lastAttemptSnapshotPath = new Path(baseDirPath, String.valueOf(lastAttempt));
      if (fs.exists(lastAttemptSnapshotPath)) {
        LOG.info("find snapshot directory for " + context.getPSAttemptId() + ", the path is " + lastAttemptSnapshotPath);
        break;
      } else {
        LOG.warn("ps: " + context.getPSAttemptId().getPsId() + ", attempt " + lastAttempt
          + " failed without write snapshots!");
        lastAttemptSnapshotPath = null;
        lastAttempt--;
      }
    }
    return lastAttemptSnapshotPath;
  }

  /**
   * Get the last written snapshot directory of the matrix
   * @param matrixId matrix id
   * @return the last written snapshot directory of the matrix
   * @throws IOException
   */
  public Path getSnapshotPath(int matrixId) throws IOException {
    Path basePath = getSnapshotBasePath();
    if(basePath == null) {
      return null;
    }

    String name = context.getMatrixMetaManager().getMatrixMeta(matrixId).getName();
    Path matrixPath = new Path(basePath, name);
    FileSystem fs = matrixPath.getFileSystem(context.getConf());

    if(!fs.exists(matrixPath)) {
      return null;
    } else {
      return basePath;
    }
  }
}
