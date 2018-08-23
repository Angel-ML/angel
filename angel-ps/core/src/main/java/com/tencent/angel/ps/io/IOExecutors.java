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


package com.tencent.angel.ps.io;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ps.PSContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

/**
 * Matrix load/save operation executors.
 */
public class IOExecutors {
  private static final Log LOG = LogFactory.getLog(IOExecutors.class);
  /**
   * PS context
   */
  private final PSContext context;

  /**
   * Worker pool
   */
  private volatile ForkJoinPool pool;

  /**
   * Create a IOExecutors
   *
   * @param context PS context
   */
  public IOExecutors(PSContext context) {
    this.context = context;
  }

  /**
   * Init
   */
  public void init() {
  }

  /**
   * Start
   */
  public void start() {
    pool = new ForkJoinPool(context.getConf()
      .getInt(AngelConf.ANGEL_PS_MATRIX_DISKIO_WORKER_POOL_SIZE,
        AngelConf.DEFAULT_ANGEL_PS_MATRIX_DISKIO_WORKER_POOL_SIZE));
  }

  /**
   * Stop
   */
  public void stop() {
    if (pool != null) {
      pool.shutdownNow();
      pool = null;
    }
  }

  /**
   * Execute a task use ForkJoin
   *
   * @param task a implementation of ForkJoinTask
   */
  public void execute(ForkJoinTask task) {
    pool.execute(task);
  }

  /**
   * Execute a task
   *
   * @param task
   */
  public void execute(Runnable task) {
    pool.execute(task);
  }
}
