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


package com.tencent.angel.ml.matrix.psf.get.getrows;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The parameter of get row function.
 */
public class GetRowsParam extends GetParam {
  /**
   * row indexes
   */
  private final int[] rowIndexes;

  /**
   * Create a new GetRowsParam.
   *
   * @param matrixId   matrix id
   * @param rowIndexes row indexes
   */
  public GetRowsParam(int matrixId, int[] rowIndexes) {
    super(matrixId);
    this.rowIndexes = rowIndexes;
  }

  /**
   * Get row indexes.
   *
   * @return List<Integer> row indexes
   */
  public int[] getRowIndexes() {
    return rowIndexes;
  }

  @Override public List<PartitionGetParam> split() {
    ArrayList<Integer> rowIdList = new ArrayList<>(rowIndexes.length);
    for (int i = 0; i < rowIndexes.length; i++) {
      rowIdList.add(rowIndexes[i]);
    }
    Map<PartitionKey, List<Integer>> parts =
      PSAgentContext.get().getMatrixMetaManager().getPartitionToRowsMap(matrixId, rowIdList);

    List<PartitionGetParam> partParams = new ArrayList<PartitionGetParam>(parts.size());

    for (Map.Entry<PartitionKey, List<Integer>> entry : parts.entrySet()) {
      partParams.add(new PartitionGetRowsParam(matrixId, entry.getKey(), entry.getValue()));
    }
    return partParams;
  }
}
