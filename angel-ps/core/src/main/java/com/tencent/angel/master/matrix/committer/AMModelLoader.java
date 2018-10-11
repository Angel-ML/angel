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


package com.tencent.angel.master.matrix.committer;

import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.matrixmeta.AMMatrixMetaManager;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.PartitionMeta;
import com.tencent.angel.model.*;
import com.tencent.angel.model.io.IOExecutors;
import com.tencent.angel.ps.ParameterServerId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Model load manager
 */
public class AMModelLoader {
  private static final Log LOG = LogFactory.getLog(AMModelLoader.class);
  private final Lock lock;

  /**
   * master context
   */
  private final AMContext context;

  /**
   * Model load contexts
   */
  private final Map<Integer, ModelLoadContext> loadContexts;

  /**
   * Load request id to result map
   */
  private final Map<Integer, ModelLoadResult> results;

  /**
   * PS id to PS sub load request context map
   */
  private Map<ParameterServerId, PSMatricesLoadContext> currentSubLoadContexts;

  /**
   * PS id to sub result map
   */
  private Map<ParameterServerId, PSMatricesLoadResult> subResults;

  /**
   * Current load request id
   */
  private int currentRequestId = -1;

  /**
   * HDFS operation executor
   */
  private IOExecutors fileOpExecutor;

  private int loadRequestIdGen = 0;

  /**
   * Is stop the dispatcher and commit tasks
   */
  private final AtomicBoolean stopped;

  /**
   * Received sub request results number
   */
  private int receivedSubResult;

  /**
   * Create a AMModelLoader
   *
   * @param context master context
   */
  public AMModelLoader(AMContext context) {
    this.context = context;
    this.stopped = new AtomicBoolean(false);
    this.loadContexts = new ConcurrentHashMap<>();
    this.results = new ConcurrentHashMap<>();
    this.lock = new ReentrantLock();
  }

  /**
   * Is a saving operation executing now
   *
   * @return true or false
   */
  public boolean isLoading() {
    try {
      lock.lock();
      return currentRequestId > 0;
    } finally {
      lock.unlock();
    }
  }

  public int load(ModelLoadContext loadContext) {
    try {
      lock.lock();
      if (isLoading()) {
        throw new IllegalStateException(
          "Angel is loading now, loading context=" + loadContexts.get(currentRequestId));
      }
      currentRequestId = loadRequestIdGen++;
      LOG.info(
        "Start to execute load request " + loadContext + " with request id=" + currentRequestId);

      // Split the user request to sub-requests to pss
      currentSubLoadContexts = split(currentRequestId, loadContext);
      subResults = new HashMap<>(currentSubLoadContexts.size());
      for (Map.Entry<ParameterServerId, PSMatricesLoadContext> entry : currentSubLoadContexts
        .entrySet()) {
        subResults.put(entry.getKey(), new PSMatricesLoadResult(entry.getValue().getRequestId(),
          entry.getValue().getSubRequestId(), LoadState.INIT));
      }
      receivedSubResult = 0;

      loadContexts.put(currentRequestId, loadContext);
      results.put(currentRequestId, new ModelLoadResult(currentRequestId));
      results.get(currentRequestId).setState(LoadState.LOADING);
      return currentRequestId;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Get load result
   *
   * @param requestId request id
   * @return save result
   */
  public ModelLoadResult getModelLoadResult(int requestId) {
    try {
      lock.lock();
      return results.get(requestId);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Get load request for a ps
   *
   * @param psId ps id
   * @return the load request for the ps
   */
  public PSMatricesLoadContext getLoadContext(ParameterServerId psId) {
    try {
      lock.lock();
      if (currentRequestId == -1) {
        return null;
      }
      return currentSubLoadContexts.get(psId);
    } finally {
      lock.unlock();
    }
  }

  /**
   * PS start loading
   *
   * @param psId         PS id
   * @param requestId    load request id
   * @param subRequestId load sub-request id
   */
  public void psLoadStart(ParameterServerId psId, int requestId, int subRequestId) {
    try {
      lock.lock();
      if (currentRequestId == requestId) {
        PSMatricesLoadResult subResult = subResults.get(psId);
        subResult.setState(LoadState.LOADING);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Get load result of the ps
   *
   * @param psId ps id
   * @return load result of the ps
   */
  public PSMatricesLoadResult getLoadResult(ParameterServerId psId) {
    try {
      lock.lock();
      if (currentRequestId == -1) {
        return null;
      }
      return subResults.get(psId);
    } finally {
      lock.unlock();
    }
  }


  /**
   * PS finish save request
   *
   * @param psId      parameter server id
   * @param subResult the result of sub save request
   */
  public void psLoadFinish(ParameterServerId psId, PSMatricesLoadResult subResult) {
    try {
      lock.lock();
      if (currentRequestId == -1 || subResult.getRequestId() != currentRequestId) {
        return;
      }

      receivedSubResult++;
      subResults.put(psId, subResult);
      if (receivedSubResult > subResults.size()) {
        ModelLoadResult result = results.get(subResult.getRequestId());
        if (canCombine()) {
          result.setState(LoadState.SUCCESS);
          loadSuccess(result);
        } else {
          String failedMsg = combineFailedLogs();
          LOG.error("PS load failed. " + failedMsg);
          loadFailed(result, failedMsg);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  private boolean canCombine() {
    boolean can = true;
    for (PSMatricesLoadResult subResult : subResults.values()) {
      can = can && (subResult.getState() == LoadState.SUCCESS);
    }
    return can;
  }

  private String combineFailedLogs() {
    StringBuilder sb = new StringBuilder();
    sb.append("Detail failed log:").append("\n");
    for (Map.Entry<ParameterServerId, PSMatricesLoadResult> entry : subResults.entrySet()) {
      if (entry.getValue().getState() == LoadState.FAILED) {
        sb.append(entry.getKey()).append(":").append(entry.getValue().getErrorMsg()).append("\n");
      }
    }
    return sb.toString();
  }

  private void loadFailed(ModelLoadResult result, String errorLog) {
    result.setState(LoadState.FAILED);
    result.setMessage(errorLog);
    currentRequestId = -1;
    receivedSubResult = 0;
  }

  private void loadSuccess(ModelLoadResult result) {
    result.setState(LoadState.SUCCESS);
    currentRequestId = -1;
    receivedSubResult = 0;
  }

  private Map<ParameterServerId, PSMatricesLoadContext> split(int requestId,
    ModelLoadContext loadContext) {
    List<MatrixLoadContext> matricesContext = loadContext.getMatricesContext();
    Map<ParameterServerId, List<PSMatrixLoadContext>> psIdToContextsMap = new HashMap<>();
    int size = matricesContext.size();
    for (int i = 0; i < size; i++) {
      Map<ParameterServerId, PSMatrixLoadContext> psIdToContextMap = split(matricesContext.get(i));
      for (Map.Entry<ParameterServerId, PSMatrixLoadContext> matrixEntry : psIdToContextMap
        .entrySet()) {
        List<PSMatrixLoadContext> contexts = psIdToContextsMap.get(matrixEntry.getKey());
        if (contexts == null) {
          contexts = new ArrayList<>();
          psIdToContextsMap.put(matrixEntry.getKey(), contexts);
        }
        contexts.add(matrixEntry.getValue());
      }
    }

    Map<ParameterServerId, PSMatricesLoadContext> ret = new HashMap<>(psIdToContextsMap.size());
    int subRequestId = 0;
    for (Map.Entry<ParameterServerId, List<PSMatrixLoadContext>> modelEntry : psIdToContextsMap
      .entrySet()) {
      ret.put(modelEntry.getKey(),
        new PSMatricesLoadContext(requestId, subRequestId++, loadContext.getLoadPath(),
          modelEntry.getValue()));
    }
    return ret;
  }

  private Map<ParameterServerId, PSMatrixLoadContext> split(MatrixLoadContext matrixLoadContext) {
    AMMatrixMetaManager matrixMetaManager = context.getMatrixMetaManager();
    MatrixMeta meta = matrixMetaManager.getMatrix(matrixLoadContext.getMatrixName());
    if (meta == null) {
      throw new IllegalStateException("Can not find matrix " + matrixLoadContext.getMatrixName());
    }

    Map<Integer, PartitionMeta> partitions = meta.getPartitionMetas();
    Map<ParameterServerId, Set<Integer>> psIdToPartIdsMap = new HashMap<>();

    for (Map.Entry<Integer, PartitionMeta> partEntry : partitions.entrySet()) {
      ParameterServerId psId = partEntry.getValue().getMasterPs();
      if (psId == null) {
        throw new IllegalStateException("Can not get ps for partition " + partEntry.getKey());
      }
      Set partIds = psIdToPartIdsMap.get(psId);
      if (partIds == null) {
        partIds = new HashSet();
        psIdToPartIdsMap.put(psId, partIds);
      }
      partIds.add(partEntry.getKey());
    }

    int matrixId = meta.getId();
    Map<ParameterServerId, PSMatrixLoadContext> ret = new HashMap<>(psIdToPartIdsMap.size());
    for (Map.Entry<ParameterServerId, Set<Integer>> entry : psIdToPartIdsMap.entrySet()) {
      List<Integer> partIds = new ArrayList<>(entry.getValue());
      partIds.sort(new Comparator<Integer>() {
        @Override public int compare(Integer id1, Integer id2) {
          return id1 - id2;
        }
      });
      PSMatrixLoadContext psMatrixLoadContext =
        new PSMatrixLoadContext(matrixId, null, partIds);
      ret.put(entry.getKey(), psMatrixLoadContext);
    }
    return ret;
  }
}
