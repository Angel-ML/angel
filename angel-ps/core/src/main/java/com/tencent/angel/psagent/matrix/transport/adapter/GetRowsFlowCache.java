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

import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.psagent.PSAgentContext;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The sub-request results cache for get rows flow
 */
public class GetRowsFlowCache extends PartitionResponseCache {
  private static final Log LOG = LogFactory.getLog(GetRowsFlowCache.class);
  /** matrix id */
  private final int matrixId;

  /** row index to the number of partitions that contain this row map */
  private final Int2IntOpenHashMap rowIndexToPartSizeCache;

  /** row index to row splits map */
  private final Int2ObjectOpenHashMap<List<ServerRow>> rowsSplitCache;

  /** sub-request future result set */
  private final ObjectOpenHashSet<Future<List<ServerRow>>> rowsSplitFutures;

  /**
   * 
   * Create a new GetRowsFlowCache.
   *
   * @param totalRequestNum total sub-requests number
   * @param matrixId matrix id
   * @param rowIndexToPartSizeCache row index to the number of partitions that contain this row map
   */
  public GetRowsFlowCache(int totalRequestNum, int matrixId,
      Int2IntOpenHashMap rowIndexToPartSizeCache) {
    super(totalRequestNum);
    this.matrixId = matrixId;
    this.rowIndexToPartSizeCache = rowIndexToPartSizeCache;
    rowsSplitCache = new Int2ObjectOpenHashMap<List<ServerRow>>();
    rowsSplitFutures = new ObjectOpenHashSet<Future<List<ServerRow>>>(totalRequestNum);
  }

  /**
   * Add sub-request future result to cache
   * 
   * @param result sub-request result future
   */
  public void addResult(Future<List<ServerRow>> result) {
    rowsSplitFutures.add(result);
  }

  private int getRowPartSize(int rowIndex) {
    if (rowIndexToPartSizeCache.containsKey(rowIndex)) {
      return rowIndexToPartSizeCache.get(rowIndex);
    } else {
      int rowPartSize =
          PSAgentContext.get().getMatrixPartitionRouter().getRowPartitionSize(matrixId, rowIndex);
      rowIndexToPartSizeCache.put(rowIndex, rowPartSize);
      return rowPartSize;
    }
  }

  /**
   * Get the rows that need to be merged
   * 
   * @return Int2ObjectOpenHashMap<List<ServerRow>> need merge rows and the splits of them
   */
  public Int2ObjectOpenHashMap<List<ServerRow>> getNeedMergeRows() {
    Int2ObjectOpenHashMap<List<ServerRow>> needMergeRowsSplit =
        new Int2ObjectOpenHashMap<List<ServerRow>>();

    ObjectIterator<Future<List<ServerRow>>> iter = rowsSplitFutures.iterator();
    while (iter.hasNext()) {
      Future<List<ServerRow>> futureResult = iter.next();
      // Check if the result of the sub-request is received
      if (futureResult.isDone()) {
        // Update received result number
        updateReceivedResponse();

        // Get row splits received
        List<ServerRow> rowSplits;
        try {
          rowSplits = futureResult.get();
        } catch (InterruptedException | ExecutionException e) {
          LOG.warn("get result from future failed.", e);
          continue;
        }

        int num = rowSplits.size();
        for (int j = 0; j < num; j++) {
          int rowIndex = rowSplits.get(j).getRowId();
          // Put the row split to the cache(row index to row splits map)
          List<ServerRow> cachedRowSplits = rowsSplitCache.get(rowIndex);
          if (cachedRowSplits == null) {
            cachedRowSplits = new ArrayList<ServerRow>();
            rowsSplitCache.put(rowIndex, cachedRowSplits);
          }
          cachedRowSplits.add(rowSplits.get(j));

          // If all splits of the row are received, means this row can be merged
          if (getRowPartSize(rowIndex) == cachedRowSplits.size()) {
            needMergeRowsSplit.put(rowIndex, rowsSplitCache.remove(rowIndex));
          }
        }

        iter.remove();
      }
    }
    return needMergeRowsSplit;
  }
}
