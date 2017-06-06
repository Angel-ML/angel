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

package com.tencent.angel.psagent.matrix.transport.adapter;

import com.tencent.angel.ml.matrix.psf.updater.base.VoidResult;
import com.tencent.angel.psagent.matrix.transport.FutureResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The result cache for UPDATER sub-requests.
 */
public class UpdaterResponseCache extends PartitionResponseCache {
  private static final Log LOG = LogFactory.getLog(UpdaterResponseCache.class);
  /** sub-request future results */
  private final List<Future<VoidResult>> futureList;

  /** sub-request results */
  private final List<VoidResult> resultList;

  /** merged result future */
  private final FutureResult<VoidResult> mergedResult;

  /**
   * Create a new UpdaterResponseCache.
   *
   * @param totalRequestNum the number of sub-request
   */
  public UpdaterResponseCache(int totalRequestNum) {
    super(totalRequestNum);
    futureList = new ArrayList<Future<VoidResult>>(totalRequestNum);
    mergedResult = new FutureResult<VoidResult>();
    resultList = new ArrayList<VoidResult>();
  }

  /**
   * Add a future result for a sub-request.
   * 
   * @param partResultFuture the future result for a sub-request
   */
  public void addResult(Future<VoidResult> result) {
    futureList.add(result);
  }

  /**
   * Add a result for a sub-request.
   * 
   * @param result the result for a sub-request
   */
  public void addResult(VoidResult result) {
    updateReceivedResponse();
    resultList.add(result);
  }

  /**
   * Check future result list, if the response of a sub-request is received, put it to sub-request
   * result list.
   */
  public void checkFutures() {
    int size = futureList.size();
    for (int i = 0; i < size; i++) {
      if (futureList.get(i).isDone()) {
        try {
          resultList.add(futureList.remove(i).get());
        } catch (InterruptedException | ExecutionException e) {
          LOG.warn("get result from future failed.", e);
        }
        size = futureList.size();
        updateReceivedResponse();
      }
    }
  }

  /**
   * Get merged future result.
   * 
   * @return GetRowResult merged future result
   */
  public FutureResult<VoidResult> getMergedResult() {
    return mergedResult;
  }

  /**
   * Set merged result.
   * 
   * @param result merged result
   */
  public void setMergedResult(VoidResult result) {
    mergedResult.set(result);
  }

  /**
   * Get sub-request results.
   * 
   * @return List<VoidResult> sub-request results
   */
  public List<VoidResult> getResultList() {
    return resultList;
  }
}
