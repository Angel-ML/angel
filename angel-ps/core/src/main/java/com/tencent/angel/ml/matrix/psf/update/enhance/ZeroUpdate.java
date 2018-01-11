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

package com.tencent.angel.ml.matrix.psf.update.enhance;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.*;
import com.tencent.angel.psagent.PSAgentContext;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Updates elements as zero.
 */
public class ZeroUpdate extends UpdateFunc {
  /**
   * Creates a new updater.
   *
   * @param param the param
   */
  public ZeroUpdate(UpdateParam param) {
    super(param);
  }
  
  /**
   * Creates a new updater.
   */
  public ZeroUpdate() {
    this(null);
  }

  /**
   * The parameter of zero updater.
   */
  public static class ZeroUpdateParam extends UpdateParam {
    /**
     * Instantiates a new Zero updater param.
     *
     * @param matrixId the matrix id
     * @param updateClock the update clock
     */
    public ZeroUpdateParam(int matrixId, boolean updateClock) {
      super(matrixId, updateClock);
    }

    @Override
    public List<PartitionUpdateParam> split() {
      List<PartitionKey> partList =
          PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
      int size = partList.size();
      List<PartitionUpdateParam> partParamList = new ArrayList<PartitionUpdateParam>(size);
      for (int i = 0; i < size; i++) {
        partParamList.add(new ZeroPartitionUpdaterParam(matrixId, partList.get(i), updateClock));
      }

      return partParamList;
    }
  }

  /**
   * The partition updater parameter.
   */
  public static class ZeroPartitionUpdaterParam extends PartitionUpdateParam {
    /**
     * Creates new partition updater parameter.
     *
     * @param matrixId the matrix id
     * @param partKey the part key
     * @param updateClock the update clock
     */
    public ZeroPartitionUpdaterParam(int matrixId, PartitionKey partKey, boolean updateClock) {
      super(matrixId, partKey, updateClock);
    }

    /**
     * Creates a new partition updater parameter by default.
     */
    public ZeroPartitionUpdaterParam() {
      super();
    }
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    ServerPartition part =
        psContext.getMatrixStorageManager()
            .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      int startRow = part.getPartitionKey().getStartRow();
      int endRow = part.getPartitionKey().getEndRow();
      for (int i = startRow; i < endRow; i++) {
        ServerRow row = part.getRow(i);
        if (row == null) {
          continue;
        }
        zero(row);
      }
    }

  }

  private void zero(ServerRow row) {
    switch (row.getRowType()) {
      case T_DOUBLE_SPARSE:
        zero((ServerSparseDoubleRow) row);
        break;

      case T_DOUBLE_DENSE:
        zero((ServerDenseDoubleRow) row);
        break;

      case T_FLOAT_DENSE:
        zero((ServerDenseFloatRow) row);
        break;

      case T_INT_SPARSE:
        zero((ServerSparseIntRow) row);
        break;

      case T_INT_DENSE:
        zero((ServerDenseIntRow) row);
        break;

      default:
        break;
    }
  }

  private void zero(ServerSparseDoubleRow row) {
    try {
      row.getLock().writeLock().lock();
      ObjectIterator<it.unimi.dsi.fastutil.ints.Int2DoubleMap.Entry> iter =
          row.getData().int2DoubleEntrySet().fastIterator();
      it.unimi.dsi.fastutil.ints.Int2DoubleMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        entry.setValue(0);
      }
    } finally {
      row.getLock().writeLock().unlock();
    }
  }

  private void zero(ServerDenseDoubleRow row) {
    try {
      row.getLock().writeLock().lock();
      byte[] data = row.getDataArray();
      Arrays.fill(data, (byte) 0);
    } finally {
      row.getLock().writeLock().unlock();
    }
  }


  private void zero(ServerDenseFloatRow row) {
    try {
      row.getLock().writeLock().lock();
      byte[] data = row.getDataArray();
      Arrays.fill(data, (byte) 0);
    } finally {
      row.getLock().writeLock().unlock();
    }
  }

  private void zero(ServerSparseIntRow row) {
    try {
      row.getLock().writeLock().lock();
      ObjectIterator<it.unimi.dsi.fastutil.ints.Int2IntMap.Entry> iter =
          row.getData().int2IntEntrySet().fastIterator();

      it.unimi.dsi.fastutil.ints.Int2IntMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        entry.setValue(0);
      }
    } finally {
      row.getLock().writeLock().unlock();
    }
  }

  private void zero(ServerDenseIntRow row) {
    try {
      row.getLock().writeLock().lock();
      byte[] data = row.getDataArray();
      Arrays.fill(data, (byte) 0);
    } finally {
      row.getLock().writeLock().unlock();
    }
  }
}
