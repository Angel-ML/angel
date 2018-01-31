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
 *
 */

package com.tencent.angel.ml.matrix.psf.get.enhance.indexed;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ml.matrix.psf.get.single.GetRowResult;
import com.tencent.angel.ps.impl.matrix.*;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.ResponseType;

import java.util.List;

public class LongIndexGetFunc extends GetFunc {
  public LongIndexGetFunc(LongIndexGetParam param) {
    super(param);
  }

  public LongIndexGetFunc( ) {
    this(null);
  }

  /**
   * Each server partition execute this function and return values of specified index.
   * @param partParam the partition parameter
   * @return values of specified index
   */
  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    ServerPartition part =
      psContext.getMatrixStorageManager()
        .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      int rowId = ((LongIndexPartGetParam) partParam).getRowId();
      PartitionKey partKey = partParam.getPartKey();
      ServerRow row = part.getRow(rowId);
      RowType rowType = row.getRowType();
      switch (rowType) {
        case T_DOUBLE_SPARSE_LONGKEY:
        case T_DOUBLE_SPARSE_LONGKEY_COMPONENT: {
          return new LongIndexGetResult(partKey, ((ServerSparseDoubleLongKeyRow) row).getValues(
            ((LongIndexPartGetParam) partParam).getIndex()));
        }

        default:
          throw new UnsupportedOperationException("Unsupport operation: update " + rowType + " to " + this.getClass().getName());
      }
    }
    return null;
  }

  /**
   * Merge all partition get result and return a sparse double vector
   * @param partResults the partition results
   * @return a merged sparse double vector
   */

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    RowType rowType = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.getMatrixId()).getRowType();

    switch (rowType) {
      case T_DOUBLE_SPARSE_LONGKEY:
        return new GetRowResult(ResponseType.SUCCESS, ValuesCombineUtils.mergeSparseDoubleVector((LongIndexGetParam) param, partResults));

      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
        return new GetRowResult(ResponseType.SUCCESS, ValuesCombineUtils.mergeSparseDoubleCompVector((LongIndexGetParam) param, partResults));

      default:
        throw new UnsupportedOperationException("Unsupport operation: update " + rowType + " to " + this.getClass().getName());
    }
  }
}
