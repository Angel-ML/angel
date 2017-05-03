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

package com.tencent.angel.psagent.matrix.transport.adapter;

import com.tencent.angel.ml.matrix.udf.getrow.GetRowResult;
import com.tencent.angel.ml.matrix.udf.getrow.PartitionGetRowResult;
import com.tencent.angel.psagent.matrix.transport.FutureResult;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The result cache for get row udf sub-requests.
 */
public class GetRowUDFCache extends PartitionResponseCache {
  private static final Log LOG = LogFactory.getLog(GetRowUDFCache.class);
  /** sub-request future results */
  private final List<Future<PartitionGetRowResult>> partitionResultFutures;

  /** sub-request results */
  private final List<PartitionGetRowResult> partitionResults;

  /** merged result future */
  private final FutureResult<GetRowResult> mergedResult;

  /**
   * Create a new GetRowUDFCache.
   *
   * @param totalRequestNum the number of sub-request
   */
  public GetRowUDFCache(int totalRequestNum) {
    super(totalRequestNum);
    partitionResultFutures = new ArrayList<Future<PartitionGetRowResult>>(totalRequestNum);
    partitionResults = new ArrayList<PartitionGetRowResult>(totalRequestNum);
    mergedResult = new FutureResult<GetRowResult>();
  }

  /**
   * Add a result for a sub-request.
   * 
   * @param result the result for a sub-request
   */
  public void addResult(PartitionGetRowResult partResult) {
    super.updateReceivedResponse();
    partitionResults.add(partResult);
  }

  /**
   * Add a future result for a sub-request.
   * 
   * @param partResultFuture the future result for a sub-request
   */
  public void addResult(Future<PartitionGetRowResult> partResultFuture) {
    partitionResultFutures.add(partResultFuture);
  }

  /**
   * Check future result list, if the response of a sub-request is received, put it to sub-request
   * result list.
   */
  public void checkFutures() {
    int size = partitionResultFutures.size();
    for (int i = 0; i < size; i++) {
      if (partitionResultFutures.get(i).isDone()) {
        try {
          addResult(partitionResultFutures.get(i).get());
        } catch (InterruptedException | ExecutionException e) {
          LOG.warn("get result from future failed.", e);
        }
        partitionResultFutures.remove(i);
        size = partitionResultFutures.size();
      }
    }
  }

  /**
   * Get merged result.
   * 
   * @return GetRowResult merged result
   */
  public GetRowResult getMergedResult() throws InterruptedException, ExecutionException {
    return mergedResult.get();
  }
}
