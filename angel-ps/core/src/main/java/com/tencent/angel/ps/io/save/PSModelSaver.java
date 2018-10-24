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


package com.tencent.angel.ps.io.save;

import com.tencent.angel.model.PSMatricesSaveContext;
import com.tencent.angel.model.PSMatricesSaveResult;
import com.tencent.angel.model.SaveState;
import com.tencent.angel.model.io.IOExecutors;
import com.tencent.angel.ps.PSContext;
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
 * PS matrix save manager
 */
public class PSModelSaver {
  private static final Log LOG = LogFactory.getLog(PSModelSaver.class);
  /**
   * PS  context
   */
  private final PSContext context;
  private final Lock readLock;
  private final Lock writeLock;

  /**
   * Model save contexts
   */
  private final Map<Integer, PSMatricesSaveContext> saveContexts;

  /**
   * Save request id to result map
   */
  private final Map<Integer, PSMatricesSaveResult> results;

  /**
   * Current saving request id
   */
  private int currentRequestId;

  /**
   * Last saving request id
   */
  private int lastRequestId;

  /**
   * the dispatcher of save tasks
   */
  private Thread saveDispatchThread;

  /**
   * HDFS operation executor
   */
  private IOExecutors fileOpExecutor;

  /**
   * Is stop the dispatcher and commit tasks
   */
  private final AtomicBoolean stopped;

  /**
   * Create a new MatrixSaver
   *
   * @param context PS context
   */
  public PSModelSaver(PSContext context) {
    this.context = context;
    saveContexts = new HashMap<>();
    results = new HashMap<>();
    stopped = new AtomicBoolean(false);
    currentRequestId = -1;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
  }

  /**
   * Save matrices
   *
   * @param saveContext save context
   */
  public void save(PSMatricesSaveContext saveContext) {
    try {
      context.getMaster().saveStart(saveContext.getRequestId(), saveContext.getSubRequestId());
    } catch (Throwable e) {
      LOG.error("send save start message to master failed ", e);
      return;
    }

    try {
      writeLock.lock();

      if (saveContexts.containsKey(saveContext.getRequestId())) {
        LOG.info("Save task " + saveContexts.get(saveContext.getRequestId()) + " is running");
        return;
      }

      if (currentRequestId != -1) {
        LOG.warn(
          "There is another save task now, just stop it " + saveContexts.get(currentRequestId));
        if (fileOpExecutor != null) {
          fileOpExecutor.shutdown();
        }
      }
      currentRequestId = saveContext.getRequestId();
      lastRequestId = currentRequestId;
      saveContexts.put(currentRequestId, saveContext);
      results.put(currentRequestId,
        new PSMatricesSaveResult(currentRequestId, saveContext.getSubRequestId(),
          SaveState.SAVING));
      save(saveContext, results.get(currentRequestId));
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Get save result
   *
   * @return save result
   */
  public PSMatricesSaveResult getResult() {
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

  private void save(PSMatricesSaveContext saveContext, PSMatricesSaveResult saveResult) {
    saveDispatchThread = new Thread(() -> {
      try {
        context.getIOExecutors().save(saveContext);
        saveSuccess(saveResult);
      } catch (Throwable x) {
        LOG.error("save task " + saveContext + " failed ", x);
        saveFailed(saveResult,
          "save task " + saveContext + " failed:" + StringUtils.stringifyException(x));
      }
    });

    saveDispatchThread.setName("save-dispatcher");
    saveDispatchThread.start();
  }

  private void saveFailed(PSMatricesSaveResult saveResult, String failedMsg) {
    try {
      writeLock.lock();
      saveResult.setState(SaveState.FAILED);
      saveResult.setErrorMsg(failedMsg);
      currentRequestId = -1;
    } finally {
      writeLock.unlock();
    }

    sendResult(saveResult);
  }

  private void saveSuccess(PSMatricesSaveResult saveResult) {
    try {
      writeLock.lock();
      saveResult.setState(SaveState.SUCCESS);
      currentRequestId = -1;
    } finally {
      writeLock.unlock();
    }

    sendResult(saveResult);
  }

  private void sendResult(PSMatricesSaveResult saveResult) {
    try {
      context.getMaster().saveFinish(saveResult);
    } catch (Throwable e) {
      LOG.error("report saving result failed ", e);
    }
  }
}
