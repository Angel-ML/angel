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

package com.tencent.angel.ml.matrix.psf.common;


import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.enhance.PartitionUpdateParam;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerSparseDoubleLongKeyRow;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import java.util.ArrayList;

public class Increment extends CommonFunc {
  public Increment(int matrixId, int[] rows, long[] cols, double[] values) {
    super(new CommonParam(matrixId, rows, cols, values));
  }

  public Increment() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow[] rows, PartitionKey key, PartitionUpdateParam param) {

  }

  @Override
  protected void doUpdate(ServerSparseDoubleLongKeyRow[] partRows, PartitionKey key, PartitionUpdateParam param) {
    CommonParam.PSFPartitionUpdateParam partParam = (CommonParam.PSFPartitionUpdateParam)param;
    int[] rows = partParam.getInts();
    long[] cols = partParam.getLongs();
    double[] values = partParam.getDoubles();

    for (ServerSparseDoubleLongKeyRow partRow: partRows) {
      int rowId = partRow.getRowId();
      ArrayList<Integer> indics = new ArrayList<Integer>();
      for (int i = 0; i < rows.length; i++) {
        if (rowId == rows[i] && cols[i] >= partRow.getStartCol() && cols[i] < partRow.getEndCol()) {
          indics.add(i);
        }
      }
      try {
        partRow.getLock().writeLock().lock();
        Long2DoubleOpenHashMap rowData = partRow.getData();
        for (Integer i: indics) {
          if (rowData.containsKey(cols[i])) {
            rowData.addTo(cols[i], values[i]);
          } else {
            rowData.put(cols[i], values[i]);
          }
        }
      } finally {
        partRow.getLock().writeLock().unlock();
      }
    }
  }

}
