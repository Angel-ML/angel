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
 */

package com.tencent.angel.psagent.matrix.transport;

import java.util.concurrent.*;

/**
 * A simple implements of Future interface. It contains a "result" member which will be set while
 * the asynchronous task run over. Once the "result" is set, the blocked get operations will be
 * waked up.
 */
public class FutureResult<T> implements Future<T> {

  /** the result of the asynchronous task */
  private volatile T result = null;

  /** counter latch */
  private final CountDownLatch counter = new CountDownLatch(1);

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return result != null;
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    if (result != null) {
      return result;
    }
    counter.await();
    return result;
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
      TimeoutException {
    if (result != null) {
      return result;
    }
    counter.await(timeout, unit);
    return result;
  }

  /**
   * Set the result of the asynchronous task.
   * 
   * @param result the result of the asynchronous task
   */
  public void set(T result) {
    this.result = result;
    counter.countDown();
  }
}
