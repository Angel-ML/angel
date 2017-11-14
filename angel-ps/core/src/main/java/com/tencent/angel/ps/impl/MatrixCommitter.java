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

package com.tencent.angel.ps.impl;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.tencent.angel.PartitionKey;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ps.impl.matrix.ServerMatrix;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.utils.HdfsUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Time;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The Matrix committer,will asynchronously commit all partitions of matrices on parameter server
 */
public class MatrixCommitter {
  private static final Log LOG = LogFactory.getLog(MatrixCommitter.class);
  private final ParameterServer ps;
  private final AtomicBoolean isCommitting = new AtomicBoolean(false);

  /**
   * Create a new Matrix committer according to parameter server
   *
   * @param ps the ps
   */
  public MatrixCommitter(ParameterServer ps) {
    this.ps = ps;
  }

  /**
   * For committing Parameter Server's matrices.
   * @param matrixIds 
   */
  public void commit(final List<Integer> matrixIds) {
    if (isCommitting.get()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("ps is commiting......");
      }
      return;
    }
    LOG.info("to start commit tasks!");
    isCommitting.set(true);
    Thread commitRunner = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          long startTime = System.currentTimeMillis();
          PSContext.get().getMatrixPartitionManager().commit(matrixIds);
          isCommitting.set(false);
          LOG.info("commit matrices use time " + (System.currentTimeMillis() - startTime)  + " ms ");
          ps.done();
        } catch (Throwable x) {
          isCommitting.set(false);
          LOG.fatal("ps commit error ", x);
          ps.failed("commit failed." + x.getMessage());
        }
      }
    });

    commitRunner.setName("commit runner");
    commitRunner.start();
  }
}
