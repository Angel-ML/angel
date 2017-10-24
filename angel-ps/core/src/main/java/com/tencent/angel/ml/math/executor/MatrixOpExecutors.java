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

package com.tencent.angel.ml.math.executor;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.psagent.PSAgentContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

/**
 * Matrix operation executor. It use ForkJoin to achieve parallel computing.
 */
public class MatrixOpExecutors {
  private static final Log LOG = LogFactory.getLog(MatrixOpExecutors.class);
  /**
   * Worker pool for forkjoin
   */
  private static final ForkJoinPool pool;
  static {
    if(PSAgentContext.get() != null && PSAgentContext.get().getPsAgent() != null) {
      pool = new ForkJoinPool(PSAgentContext.get().getConf()
        .getInt(AngelConf.ANGEL_WORKER_MATRIX_EXECUTORS_NUM,
          AngelConf.DEFAULT_ANGEL_WORKER_MATRIX_EXECUTORS_NUM));
    } else {
      pool = new ForkJoinPool(AngelConf.DEFAULT_ANGEL_WORKER_MATRIX_EXECUTORS_NUM);
    }
  }

  /**
   * Execute a task use ForkJoin
   *
   * @param task a implementation of ForkJoinTask
   */
  public static void execute(ForkJoinTask task) {
    pool.execute(task);
  }
}
