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


package com.tencent.angel.psagent.matrix.transport;

import com.tencent.angel.exception.PSRPCException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A simple implements of Future interface. It contains a "result" member which will be set while
 * the asynchronous task run over. Once the "result" is set, the blocked get operations will be
 * waked up.
 *
 * <p>Thread-safety / write-once guarantee: it is possible for two threads to attempt to terminate
 * the same {@code FutureResult} at the same time, e.g. the response dispatcher delivers a SUCCESS
 * just as the timeout checker raises a TIMEOUT, or the channel-closed event runs concurrently
 * with a real response. Without protection the {@code result} / {@code exeExp} fields would
 * race and the caller could observe a result silently overwritten by a contradicting failure
 * (or vice versa). The {@link AtomicBoolean} below ensures only the first terminating call
 * actually mutates state and counts down the latch; subsequent calls are ignored.</p>
 */
public class FutureResult<T> implements Future<T> {

  /**
   * the result of the asynchronous task
   */
  private volatile T result = null;

  private volatile ExecutionException exeExp = null;

  /**
   * Single-shot guard: ensures {@link #set} / {@link #setExecuteException} take effect at most
   * once. Mutually exclusive winners; later callers no-op.
   */
  private final AtomicBoolean done = new AtomicBoolean(false);

  /**
   * counter latch
   */
  private final CountDownLatch counter = new CountDownLatch(1);

  @Override public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override public boolean isCancelled() {
    return false;
  }

  @Override public boolean isDone() {
    return done.get();
  }

  @Override public T get() throws InterruptedException, ExecutionException {
    if (result != null) {
      return result;
    }

    if(exeExp != null) {
      throw new ExecutionException(exeExp);
    }
    counter.await();

    if(exeExp != null) {
      throw new ExecutionException(exeExp);
    }

    return result;
  }

  @Override public T get(long timeout, TimeUnit unit)
    throws InterruptedException, ExecutionException, TimeoutException {
    if (result != null) {
      return result;
    }

    if(exeExp != null) {
      throw new ExecutionException(exeExp);
    }

    boolean ret = counter.await(timeout, unit);
    if(!ret) {
      throw new TimeoutException("Wait result timeout, timeout = " + timeout + " " + unit.name());
    }

    if(exeExp != null) {
      throw new ExecutionException(exeExp);
    }

    return result;
  }

  /**
   * Set the result of the asynchronous task. No-op if already terminated by a previous
   * {@code set} or {@code setExecuteException} call.
   *
   * @param result the result of the asynchronous task
   */
  public void set(T result) {
    if (done.compareAndSet(false, true)) {
      this.result = result;
      counter.countDown();
    }
  }

  /**
   * Set the execution exception, if you task executes failed, you can use it to break the
   * waiting task. No-op if already terminated by a previous {@code set} or
   * {@code setExecuteException} call.
   *
   * @param e execution exception
   */
  public void setExecuteException(ExecutionException e) {
    if (done.compareAndSet(false, true)) {
      this.exeExp = e;
      counter.countDown();
    }
  }


  /**
   * Set the execution error log, if you task executes failed, you can use it to break the waiting task
   * @param errorLog execution error log
   */
  public void setExecuteError(String errorLog) {
    setExecuteException(new ExecutionException(new PSRPCException(errorLog)));
  }
}
