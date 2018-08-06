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

package com.tencent.angel.ml.matrix.psf.aggr;


import com.tencent.angel.ml.matrix.psf.aggr.enhance.FullAggrFunc;
import com.tencent.angel.ml.matrix.psf.aggr.enhance.FullAggrResult;
import com.tencent.angel.ml.matrix.psf.aggr.enhance.FullPartitionAggrResult;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;

import java.util.List;

/**
 * `FullPull` can pull the whole matrix to local.
 */
public class FullPull  extends FullAggrFunc {
  public FullPull(int matrixId) {
    super(matrixId);
  }

  public FullPull() {
    super();
  }

  @Override
  protected double[][] doProcess(ServerDenseDoubleRow[] rows) {
    int rowNum = rows.length;
    int colNum = rows[0].size();
    double[][] result = new double[rowNum][colNum];
    for (int i = 0; i < rowNum; i++) {
      for (int j = 0; j < colNum; j++) {
        result[i][j] = rows[i].getData().get(j);
      }
    }
    return result;
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    int endRow = -1;
    int endCol = -1;
    for (PartitionGetResult partResult: partResults) {
      FullPartitionAggrResult result = (FullPartitionAggrResult)partResult;
      int[] partInfo = result.getPartInfo();
      assert(partInfo.length == 4);
      if (endRow < partInfo[1]) endRow = partInfo[1];
      if (endCol < partInfo[3]) endCol = partInfo[3];
    }

    double[][] result = new double[endRow][endCol];
    for (PartitionGetResult partResult: partResults) {
      FullPartitionAggrResult aggrResult = (FullPartitionAggrResult)partResult;
      double[][] pResult = aggrResult.getResult();
      int[] partInfo = aggrResult.getPartInfo();
      int thisStartRow = partInfo[0];
      int thisEndRow = partInfo[1];
      int thisStartCol = partInfo[2];
      int thisEndCol = partInfo[3];
      int thisColSize = thisEndCol - thisStartCol;
      for (int i = 0; i < pResult.length; i++) {
        System.arraycopy(pResult[i], 0, result[thisStartRow + i], thisStartCol, thisColSize);
      }
    }
    return new FullAggrResult(result);
  }
}
