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

package com.tencent.angel.master.matrix.committer;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.ml.math.executor.MatrixOpExecutors;
import com.tencent.angel.ps.impl.PSContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

/**
 * Master Matrix disk io operation executors.
 */
public class MatrixDiskIOExecutors {
  private static final Log LOG = LogFactory.getLog(MatrixOpExecutors.class);
  private AMContext amContext;
  private final ForkJoinPool pool;

  public MatrixDiskIOExecutors(AMContext context) {
    amContext = context;
    pool = new ForkJoinPool(amContext.getConf()
      .getInt(AngelConf.ANGEL_AM_MATRIX_DISKIO_WORKER_POOL_SIZE,
        AngelConf.DEFAULT_ANGEL_AM_MATRIX_DISKIO_WORKER_POOL_SIZE));
  }

  /**
   * Execute a task use ForkJoin
   *
   * @param task a implementation of ForkJoinTask
   */
  public void execute(ForkJoinTask task) {
    pool.execute(task);
  }

  public void shutdown() {
    pool.shutdownNow();
  }
}
