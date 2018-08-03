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

package com.tencent.angel.model.io;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

/**
 * Master Matrix disk io operation executors.
 */
public class IOExecutors {
  private static final Log LOG = LogFactory.getLog(IOExecutors.class);
  private final int poolSize;
  private volatile ForkJoinPool pool;

  public IOExecutors(int poolSize) {
    this.poolSize = poolSize;
  }

  public void init() {

  }

  public void start() {
    pool = new ForkJoinPool(poolSize);
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
   * @param task a implementation of Runnable
   */
  public void execute(Runnable task) { pool.execute(task);}

  /**
   * Shut down all workers
   */
  public void shutdown() {
    if(pool != null) {
      pool.shutdownNow();
      pool = null;
    }
  }
}
