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


package com.tencent.angel.psagent.matrix.transport.adapter;

import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.psagent.PSAgentContext;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * The sub-request results cache for get rows flow
 */
public class GetRowsFlowCache extends PartitionResponseCache<List<ServerRow>> {
  private static final Log LOG = LogFactory.getLog(GetRowsFlowCache.class);
  /**
   * matrix id
   */
  private final int matrixId;

  /**
   * row index to the number of partitions that contain this row map
   */
  private final Int2IntOpenHashMap rowIndexToPartSizeCache;

  /**
   * Received row splits cache
   */
  private final Int2ObjectOpenHashMap<List<ServerRow>> rowsSplitCache;

  /**
   * Need merge row splits
   */
  private final LinkedBlockingQueue<RowMergeItem> needMergeRowsQueue;

  /**
   * The number of received sub responses
   */
  private int receivedSubResponse;

  /**
   * Create a new GetRowsFlowCache.
   *
   * @param totalRequestNum         total sub-requests number
   * @param matrixId                matrix id
   * @param rowIndexToPartSizeCache row index to the number of partitions that contain this row map
   */
  public GetRowsFlowCache(int totalRequestNum, int matrixId,
    Int2IntOpenHashMap rowIndexToPartSizeCache) {
    super(totalRequestNum, 0);
    this.matrixId = matrixId;
    this.rowIndexToPartSizeCache = rowIndexToPartSizeCache;
    rowsSplitCache = new Int2ObjectOpenHashMap<>();
    needMergeRowsQueue = new LinkedBlockingQueue<>();
    receivedSubResponse = 0;
  }

  private int getRowPartSize(int rowIndex) {
    if (rowIndexToPartSizeCache.containsKey(rowIndex)) {
      return rowIndexToPartSizeCache.get(rowIndex);
    } else {
      int rowPartSize =
        PSAgentContext.get().getMatrixMetaManager().getRowPartitionSize(matrixId, rowIndex);
      rowIndexToPartSizeCache.put(rowIndex, rowPartSize);
      return rowPartSize;
    }
  }

  @Override public void addSubResponse(List<ServerRow> rowSplits) {
    try {
      lock.lock();
      receivedSubResponse++;
      int size = rowSplits.size();
      for (int j = 0; j < size; j++) {
        int rowIndex = rowSplits.get(j).getRowId();

        // Put the row split to the cache(row index to row splits map)
        List<ServerRow> cachedRowSplits = rowsSplitCache.get(rowIndex);
        if (cachedRowSplits == null) {
          cachedRowSplits = new ArrayList<>();
          rowsSplitCache.put(rowIndex, cachedRowSplits);
        }
        cachedRowSplits.add(rowSplits.get(j));

        // If all splits of the row are received, means this row can be merged
        if (getRowPartSize(rowIndex) == cachedRowSplits.size()) {
          needMergeRowsQueue.offer(new RowMergeItem(rowIndex, rowsSplitCache.remove(rowIndex)));
        }
      }
    } finally {
      lock.unlock();
    }
  }

  @Override public boolean isReceivedOver() {
    try {
      lock.lock();
      return receivedSubResponse >= totalRequestNum;
    } finally {
      lock.unlock();
    }
  }

  @Override public int getReceivedResponseNum() {
    try {
      lock.lock();
      return receivedSubResponse;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Get need merge rows
   *
   * @return need merge rows
   */
  public List<RowMergeItem> getCanMergeRows() {
    int size = needMergeRowsQueue.size();
    if (size == 0) {
      return null;
    } else {
      List<RowMergeItem> ret = new ArrayList<>(size);
      while (true) {
        RowMergeItem item = needMergeRowsQueue.poll();
        if (item == null) {
          return ret;
        }
        ret.add(item);
      }
    }
  }
}
