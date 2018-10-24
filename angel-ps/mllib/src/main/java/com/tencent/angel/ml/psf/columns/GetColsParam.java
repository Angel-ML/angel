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


package com.tencent.angel.ml.psf.columns;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.utils.ArrayCopy;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.IntLongVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;

public class GetColsParam extends GetParam {

  int[] rows;
  long[] cols;
  InitFunc func;

  public GetColsParam(int matId, int[] rows, long[] cols, InitFunc func) {
    super(matId);
    this.rows = rows;
    this.cols = cols;
    this.func = func;
  }

  public GetColsParam(int matId, int[] rows, long[] cols) {
    this(matId, rows, cols, null);
  }

  public GetColsParam(int matId, int[] rows, Vector cols, InitFunc func) {
    this(matId, rows, getCols(cols), func);
  }

  public GetColsParam(int matId, int[] rows, Vector cols) {
    this(matId, rows, getCols(cols), null);
  }

  // TODO: optimize int key indices
  static long [] getCols(Vector colVec) {
    if (colVec instanceof IntLongVector) {
      return ((IntLongVector) colVec).getStorage().getValues();
    } else {
      int[] values = ((IntIntVector) colVec).getStorage().getValues();
      long [] cols = new long[values.length];
      ArrayCopy.copy(values, cols);
      return cols;
    }
  }

  @Override public List<PartitionGetParam> split() {
    List<PartitionKey> pkeys = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    List<PartitionGetParam> params = new ArrayList<>();
    int start = 0, end = 0;

    for (PartitionKey pkey : pkeys) {
      long startCol = pkey.getStartCol();
      long endCol = pkey.getEndCol();
      if (start < cols.length && cols[start] >= startCol) {
        while (end < cols.length && cols[end] < endCol)
          end++;
        long[] part = new long[end - start];
        System.arraycopy(cols, start, part, 0, end - start);
        params.add(new PartitionGetColsParam(matrixId, pkey, rows, part, func));
        start = end;
      }
    }
    return params;
  }
}
