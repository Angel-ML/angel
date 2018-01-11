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

import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.psagent.matrix.transport.FutureResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The result cache for GET_ROW sub-requests.
 */
public class GetRowPipelineCache extends PartitionResponseCache {
  private static final Log LOG = LogFactory.getLog(GetRowPipelineCache.class);
  /** sub-request future results */
  private final List<Future<ServerRow>> rowSplitsFutures;

  /** sub-request result queue */
  private final LinkedBlockingQueue<ServerRow> rowSplits;

  /** merge startup ratio */
  private final double startMergeRatio;

  /** row type */
  private final RowType rowType;

  /** merged result */
  private final FutureResult<TVector> mergedResult;
  private final Lock distinctLock;
  private final ReentrantReadWriteLock lock;

  /**
   * 
   * Create a new GetRowPipelineCache.
   *
   * @param totalRequestNum the number of sub-requests
   * @param rowType row type
   */
  public GetRowPipelineCache(int totalRequestNum, RowType rowType) {
    super(totalRequestNum);
    this.rowType = rowType;

    rowSplitsFutures = new ArrayList<Future<ServerRow>>(totalRequestNum);
    rowSplits = new LinkedBlockingQueue<ServerRow>();

    startMergeRatio = 0.3;
    mergedResult = new FutureResult<TVector>();;
    distinctLock = new ReentrantLock();
    lock = new ReentrantReadWriteLock();
  }

  /**
   * Add the result of a sub-request to the result queue.
   * 
   * @param rowSplit row split, the result of a sub-request
   */
  public void addRowSplit(ServerRow rowSplit) {
    super.updateReceivedResponse();
    rowSplits.add(rowSplit);
  }

  /**
   * Add the future result of a sub-request
   * 
   * @param rowSplit the result future of a sub-request
   */
  public void addRowSplit(Future<ServerRow> rowSplit) {
    try {
      lock.writeLock().lock();
      rowSplitsFutures.add(rowSplit);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void checkFutures() {
    try {
      lock.readLock().lock();
      int size = rowSplitsFutures.size();
      for (int i = 0; i < size; i++) {
        if (rowSplitsFutures.get(i).isDone()) {
          try {
            addRowSplit(rowSplitsFutures.get(i).get());
          } catch (InterruptedException | ExecutionException e) {
            LOG.warn("get result from future failed.", e);
          }
          rowSplitsFutures.remove(i);
          size = rowSplitsFutures.size();
        }
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Can startup row splits merger now?
   * 
   * @return boolean true means can startup merger
   */
  public boolean canStartMerge() {
    // Check futures, if the result of a sub-request is received, put it to the result queue
    checkFutures();

    // Now we just support pipelined row splits merging for dense type row
    if (rowType == RowType.T_DOUBLE_DENSE || rowType == RowType.T_INT_DENSE
        || rowType == RowType.T_FLOAT_DENSE) {
      return getProgress() >= startMergeRatio;
    } else {
      return isReceivedOver();
    }
  }

  /**
   * Get a row split from the result queue.
   * 
   * @return ServerRow a row split
   * @throws InterruptedException interrupted when taking a row split from block queue
   */
  public ServerRow poll() throws InterruptedException {
    if (isReceivedOver() && rowSplits.isEmpty()) {
      return null;
    }

    return rowSplits.take();
  }

  /**
   * Get row type
   * 
   * @return RowType row type
   */
  public RowType getRowType() {
    return rowType;
  }

  /**
   * Get merged future result.
   * 
   * @return FutureResult<TVector> merged result future
   */
  public FutureResult<TVector> getMergedResult() {
    return mergedResult;
  }

  /**
   * Set merged result.
   * 
   * @param mergedResult merged result
   */
  public void setMergedResult(TVector mergedResult) {
    this.mergedResult.set(mergedResult);
  }

  @Override
  public String toString() {
    return "GetRowSplitPipelineCache [rowSplits len=" + rowSplits.size()
        + ", rowSplitsFutures len=" + rowSplitsFutures.size() + ", startMergeRitio="
        + startMergeRatio + ", rowType=" + rowType + ", mergedResult=" + mergedResult
        + ", toString()=" + super.toString() + "]";
  }

  public Lock getDistinctLock() {
    return distinctLock;
  }
}
