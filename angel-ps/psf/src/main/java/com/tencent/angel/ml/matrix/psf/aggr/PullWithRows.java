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

package com.tencent.angel.ml.matrix.psf.aggr;


import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.aggr.enhance.*;
import com.tencent.angel.ml.matrix.psf.common.Utils;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;

import java.nio.DoubleBuffer;
import java.util.*;

/**
 * `PullWithCols` can pull specific cols in a row.
 */
public class PullWithRows extends GetFunc {
  int[] rows;
  public PullWithRows(int matrixId, int[] rows) {
    super(new MultiAggrParam(matrixId, rows));
    this.rows = rows;
  }

  public PullWithRows() {
    super(null);
  }


  SBPartitionAggrResult doProcess(ServerPartition part, int[] rows) {
    PartitionKey key = part.getPartitionKey();
    int startRow = key.getStartRow();
    int endRow = key.getEndRow();

    int count = 0;
    for (int rowId: rows) {
      if (rowId >= startRow && rowId < endRow) {
        count++;
      }
    }
    int[] resRows = new int[count];
    double[][] resData = new double[count][];
    int index = 0;
    for (int rowId: rows) {
      if (rowId >= startRow && rowId < endRow) {
        ServerDenseDoubleRow row = (ServerDenseDoubleRow) part.getRow(rowId);
        DoubleBuffer data = row.getData();
        double[] array;
        if (data.hasArray()) {
          array = data.array();
        } else {
          array = new double[row.size()];
          for (int i = 0; i < row.size(); i++) {
            array[i] = data.get(i);
          }
        }
        resRows[index] = rowId;
        resData[index++] = array;
      }
    }
    return new SBPartitionAggrResult(resRows, resData);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partKey) {
    ServerPartition part = psContext.getMatrixStorageManager().getPart(partKey.getMatrixId(), partKey.getPartKey().getPartitionId());
    int[] rowIds = ((MultiAggrParam.MultiPartitionAggrParam) partKey).getRowIds();
    if (part == null) return null;

    return doProcess(part, rowIds);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    int count = 0;
    for (PartitionGetResult part: partResults) {
      SBPartitionAggrResult partResult = (SBPartitionAggrResult) part;
      count += partResult.getRowIds().length;
    }
    int[] resRows = new int[count];
    double[][] resData = new double[count][];
    int index = 0;

    for (PartitionGetResult part: partResults) {
      SBPartitionAggrResult partResult = (SBPartitionAggrResult) part;
      int[] keys = partResult.getRowIds();
      double[][] values = partResult.getData();
      assert (keys.length == values.length);
      for (int i = 0; i < keys.length; i++) {
        resRows[index] = keys[i];
        resData[index++] = values[i];
      }
    }

    return new SBAggrResult(resRows, resData);
  }
}
