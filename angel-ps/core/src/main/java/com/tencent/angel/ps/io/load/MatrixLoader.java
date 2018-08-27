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

import com.tencent.angel.model.LoadState;
import com.tencent.angel.model.PSMatricesLoadContext;
import com.tencent.angel.model.PSMatricesLoadResult;
import com.tencent.angel.model.io.IOExecutors;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.io.save.MatrixSaver;
import com.tencent.angel.utils.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * PS matrix load manager
 */
public class MatrixLoader {
  private static final Log LOG = LogFactory.getLog(MatrixSaver.class);
  /**
   * PS context
   */
  private final PSContext context;
  private final Lock readLock;
  private final Lock writeLock;

  /**
   * Model load contexts
   */
  private final Map<Integer, PSMatricesLoadContext> loadContexts;

  /**
   * Load request id to result map
   */
  private final Map<Integer, PSMatricesLoadResult> results;

  /**
   * Current loading request id
   */
  private int currentRequestId;

  /**
   * Last saving request id
   */
  private int lastRequestId;


  /**
   * the dispatcher of load tasks
   */
  private Thread loadDispatchThread;

  /**
   * HDFS operation executor
   */
  private IOExecutors fileOpExecutor;

  /**
   * Is stop the dispatcher and commit tasks
   */
  private final AtomicBoolean stopped;

  /**
   * Create a new MatrixLoader
   *
   * @param context PS context
   */
  public MatrixLoader(PSContext context) {
    this.context = context;
    loadContexts = new HashMap<>();
    results = new HashMap<>();
    stopped = new AtomicBoolean(false);
    currentRequestId = -1;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
  }

  /**
   * Load matrices
   *
   * @param loadContext load context
   */
  public void load(PSMatricesLoadContext loadContext) {
    try {
      context.getMaster().loadStart(loadContext.getRequestId(), loadContext.getSubRequestId());
    } catch (Throwable e) {
      LOG.error("send load start message to master failed ", e);
      return;
    }
    try {
      writeLock.lock();
      if (loadContext.getRequestId() == currentRequestId) {
        LOG.info("Load task " + loadContexts.get(currentRequestId) + " is running");
        return;
      }

      if (currentRequestId != -1) {
        LOG.warn(
          "There is another load task now, just stop it " + loadContexts.get(currentRequestId));
        if (fileOpExecutor != null) {
          fileOpExecutor.shutdown();
        }
      }
      currentRequestId = loadContext.getRequestId();
      lastRequestId = currentRequestId;
      loadContexts.put(currentRequestId, loadContext);
      results.put(currentRequestId,
        new PSMatricesLoadResult(currentRequestId, loadContext.getSubRequestId(),
          LoadState.LOADING));
      load(loadContext, results.get(currentRequestId));
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Get load result
   *
   * @return load result
   */
  public PSMatricesLoadResult getResult() {
    try {
      readLock.lock();
      if (currentRequestId > 0) {
        return results.get(currentRequestId);
      } else if (lastRequestId > 0) {
        return results.get(lastRequestId);
      } else {
        return null;
      }
    } finally {
      readLock.unlock();
    }
  }

  private void load(PSMatricesLoadContext loadContext, PSMatricesLoadResult loadResult) {
    loadDispatchThread = new Thread(() -> {
      try {
        context.getMatrixStorageManager().load(loadContext);
        loadSuccess(loadResult);
      } catch (Throwable x) {
        LOG.error("Load task " + loadContext + " failed ", x);
        loadFailed(loadResult,
          "Load task " + loadContext + " failed:" + StringUtils.stringifyException(x));
      }
    });

    loadDispatchThread.setName("load-dispatcher");
    loadDispatchThread.start();
  }

  private void loadFailed(PSMatricesLoadResult loadResult, String failedMsg) {
    try {
      writeLock.lock();
      loadResult.setState(LoadState.FAILED);
      loadResult.setErrorMsg(failedMsg);
      currentRequestId = -1;
    } finally {
      writeLock.unlock();
    }

    sendResult(loadResult);
  }

  private void loadSuccess(PSMatricesLoadResult loadResult) {
    try {
      writeLock.lock();
      loadResult.setState(LoadState.SUCCESS);
      currentRequestId = -1;
    } finally {
      writeLock.unlock();
    }

    sendResult(loadResult);
  }

  private void sendResult(PSMatricesLoadResult saveResult) {
    try {
      context.getMaster().loadFinish(saveResult);
    } catch (Throwable e) {
      LOG.error("report saving result failed ", e);
    }
  }
}