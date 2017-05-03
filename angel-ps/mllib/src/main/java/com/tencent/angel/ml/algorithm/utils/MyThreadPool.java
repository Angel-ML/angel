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

package com.tencent.angel.ml.algorithm.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Description: Thread pool
 */
public class MyThreadPool {

  private static Log logger = LogFactory.getLog(MyThreadPool.class);
  private final ExecutorService pool;

  public MyThreadPool(int maxThreadNum) {
    this.pool = Executors.newFixedThreadPool(maxThreadNum);
  }

  /**
   * Gets thread pool.
   *
   * @return the pool
   */
  public ExecutorService getPool() {
    return pool;
  }
}


// ~ Formatted by Jindent --- http://www.jindent.com
