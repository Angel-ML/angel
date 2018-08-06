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

package com.tencent.angel.ps.impl;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.model.output.format.ModelFilesConstent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The Matrix committer,will asynchronously commit all partitions of matrices on parameter server
 */
public class MatrixCommitter {
  private static final Log LOG = LogFactory.getLog(MatrixCommitter.class);
  private final AtomicBoolean canCommit = new AtomicBoolean(true);
  private final PSContext context;
  private volatile Thread commitRunner;

  /**
   * Create a new Matrix committer according to parameter server
   *
   * @param context the ps context
   */
  public MatrixCommitter(PSContext context) {
    this.context = context;
  }

  /**
   * For committing Parameter Server's matrices.
   * @param matrixPartitions matrix id -> need save matrices map
   */
  public void commit(Map<Integer, List<Integer>> matrixPartitions) {
    if(!canCommit.getAndSet(false)) {
      LOG.debug("Model is saved, just return");
      return;
    }

    LOG.info("to start commit tasks!");
    commitRunner = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          long startTime = System.currentTimeMillis();
          Configuration conf = context.getConf();
          String outputPath = conf.get(AngelConf.ANGEL_JOB_TMP_OUTPUT_PATH);
          Path baseDir = new Path(new Path(outputPath, ModelFilesConstent.resultDirName), context.getPs().getServerId().toString());
          context.getMatrixStorageManager().save(matrixPartitions, baseDir);
          LOG.info("commit matrices use time " + (System.currentTimeMillis() - startTime)  + " ms ");
          context.getPs().done();
        } catch (Throwable x) {
          LOG.fatal("ps commit error ", x);
          context.getPs().failed("commit failed." + x.getMessage());
        }
      }
    });

    commitRunner.setName("commit runner");
    commitRunner.start();
  }

  public void stop() {
    if(commitRunner != null) {
      commitRunner.interrupt();
    }
  }
}
