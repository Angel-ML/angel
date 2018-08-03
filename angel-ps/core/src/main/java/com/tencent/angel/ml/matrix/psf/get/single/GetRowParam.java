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

package com.tencent.angel.ml.matrix.psf.get.single;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;

/**
 * The parameter of get row function.
 */
public class GetRowParam extends GetParam {

  /** row index */
  private final int rowIndex;

  /**
   * Create a new GetRowParam.
   *
   * @param matrixId matrix id
   * @param rowIndex row index
   */
  public GetRowParam(int matrixId, int rowIndex) {
    super(matrixId);
    this.rowIndex = rowIndex;
  }

  /**
   * Get row index.
   *
   * @return int row index.
   */
  public int getRowIndex() {
    return rowIndex;
  }

  @Override
  public List<PartitionGetParam> split() {
    List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId, rowIndex);
    int size = parts.size();

    List<PartitionGetParam> partParams = new ArrayList<PartitionGetParam>(size);

    for (int i = 0; i < size; i++) {
      partParams.add(new PartitionGetRowParam(matrixId, rowIndex, parts.get(i)));
    }

    return partParams;
  }

  @Override public String toString() {
    return "GetRowParam{" + "rowIndex=" + rowIndex + "} " + super.toString();
  }
}
