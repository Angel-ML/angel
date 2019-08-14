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


package com.tencent.angel.ps.io.load;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.model.output.format.ModelFilesConstent;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.utils.Sort;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
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
   *
   * @param context PS context
   */
  public SnapshotRecover(PSContext context) {
    this.context = context;
    String outputDir = context.getConf().get(AngelConf.ANGEL_JOB_TMP_OUTPUT_PATH);
    baseDirPath = new Path(
      outputDir + Path.SEPARATOR + ModelFilesConstent.snapshotDirName + Path.SEPARATOR + context
        .getPSAttemptId().getPsId());
  }

  /**
   * Get the last written snapshot directory
   *
   * @return the last written snapshot directory
   * @throws IOException
   */
  public Path getSnapshotBasePath() throws IOException {
    return baseDirPath;
  }

  /**
   * Get the last written snapshot directory of the matrix
   *
   * @param matrixId matrix id
   * @return the last written snapshot directory of the matrix
   * @throws IOException
   */
  public Path getSnapshotPath(int matrixId) throws IOException {
    String name = context.getMatrixMetaManager().getMatrixMeta(matrixId).getName();
    Path matrixPath = new Path(baseDirPath, name);
    FileSystem fs = matrixPath.getFileSystem(context.getConf());

    // If matrix checkpoint path not exist, just return null
    if (!fs.exists(matrixPath)) {
      return null;
    }

    // Return the path with maximum checkpoint id
    FileStatus[] status = fs.listStatus(matrixPath);
    if(status == null || status.length == 0) {
      return null;
    }

    int maxCheckpointId = Integer.MIN_VALUE;
    int index = 0;
    for (int i = 0; i < status.length; i++) {
      int id = 0;
      try {
        id = Integer.valueOf(status[i].getPath().getName());
      } catch (Throwable x) {
        LOG.warn("Path " + status[i].getPath().toString()
            + " is not a valid checkpoint path");
      }

      if(id > maxCheckpointId) {
        maxCheckpointId = id;
        index = i;
      }
    }

    return status[index].getPath();
  }
}
