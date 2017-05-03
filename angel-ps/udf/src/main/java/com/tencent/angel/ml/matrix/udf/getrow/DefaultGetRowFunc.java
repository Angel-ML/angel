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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.matrix.udf.getrow;

import java.util.ArrayList;
import java.util.List;

import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.psagent.matrix.ResponseType;
import com.tencent.angel.psagent.matrix.transport.adapter.RowSplitCombineUtils;

/**
 * The function of default get row.
 */
public class DefaultGetRowFunc extends BaseGetRowFunc{

  /**
   * Creates a new function.
   *
   * @param param the param
   */
  public DefaultGetRowFunc(GetRowParam param) {
    super(param);
  }

  /**
   *  Creates a new function by default.
   */
  public DefaultGetRowFunc() {
    super();
  }

  @Override
  public PartitionGetRowResult partitionGet(PartitionGetRowParam param) {
    ServerRow row =
        PSContext
            .get()
            .getMatrixPartitionManager()
            .getRow(param.getMatrixId(), param.getRowIndex(), param.getPartKey().getPartitionId());
    return new PartitionGetRowResult(row);
  }

  @Override
  public GetRowResult merge(List<PartitionGetRowResult> partResults) {
    int size = partResults.size();
    List<ServerRow> rowSplits = new ArrayList<ServerRow>(size);
    for (int i = 0; i < size; i++) {
      rowSplits.add(partResults.get(i).getRowSplit());
    }

    return new GetRowResult(ResponseType.SUCCESS, RowSplitCombineUtils.combineServerRowSplits(
        rowSplits, param.matrixId, param.rowIndex));
  }
}
