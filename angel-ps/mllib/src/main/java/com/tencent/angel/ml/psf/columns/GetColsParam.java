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
import com.tencent.angel.ml.math2.storage.VectorStorage;
import com.tencent.angel.ml.math2.utils.ArrayCopy;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.IntLongVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GetColsParam extends GetParam {
  Log LOG = LogFactory.getLog(GetColsParam.class);

  int[] rows;
  long[] cols;

  public GetColsParam(int matId, int[] rows, long[] cols) {
    super(matId);
    this.rows = rows;
    this.cols = cols;
  }

  public GetColsParam(int matId, int[] rows, Vector cols) {
    super(matId);
    this.rows = rows;

    if (cols instanceof IntLongVector) {
      this.cols = ((IntLongVector) cols).getStorage().getValues();
    } else {
      int[] values = ((IntIntVector) cols).getStorage().getValues();
      this.cols = new long[values.length];
      // StringBuffer sb = new StringBuffer();
      // sb.append("GetColsParam values ");
      ArrayCopy.copy(values, this.cols);
      // LOG.error(sb.toString());
    }

    // StringBuffer sb = new StringBuffer();
    // sb.append("GetColsParams ");
    // for (int i = 0; i < this.cols.length; i ++) {
    //    sb.append(this.cols[i] + " ");
    // }
    // LOG.error(sb.toString());
  }

  @Override public List<PartitionGetParam> split() {
    List<PartitionKey> pkeys = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    // Arrays.sort(rows);
    List<PartitionGetParam> params = new ArrayList<>();
    int start = 0, end = 0;
    // int sum = 0;
    // System.out.println("pkeys.size=" + pkeys.size());

    // params.add(new PartitionGetColsParam(matrixId, pkeys.get(0), rows, cols));
    for (PartitionKey pkey : pkeys) {
      long startCol = pkey.getStartCol();
      long endCol = pkey.getEndCol();
      if (start < cols.length && cols[start] >= startCol) {
        while (end < cols.length && cols[end] < endCol)
          end++;
        long[] part = new long[end - start];
        System.arraycopy(cols, start, part, 0, end - start);
        params.add(new PartitionGetColsParam(matrixId, pkey, rows, part));
        start = end;
        // sum += part.length;
      }
    }

    // LOG.info("split length = " + sum + ", cols = " + cols.length);
    return params;
  }
}
