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


package com.tencent.angel.ml.matrix.psf.get.indexed;

import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ml.matrix.psf.get.getrow.GetRowResult;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.vector.*;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.ResponseType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

public class LongIndexGet extends GetFunc {
  private static final Log LOG = LogFactory.getLog(LongIndexGet.class);

  public LongIndexGet(LongIndexGetParam param) {
    super(param);
  }

  public LongIndexGet() {
    this(null);
  }

  /**
   * Each server partition execute this function and return values of specified index.
   *
   * @param partParam the partition parameter
   * @return values of specified index
   */
  @Override public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    long startTs = System.currentTimeMillis();
    RowBasedPartition part = (RowBasedPartition)psContext.getMatrixStorageManager()
      .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if(part == null) {
      throw new RuntimeException("Can not find partition " + partParam.getPartKey().getPartitionId());
    }

    int rowId = ((LongIndexPartGetParam) partParam).getRowId();
    long[] indexes = ((LongIndexPartGetParam) partParam).getIndex();
    ServerRow row = part.getRow(rowId);

    if(row == null) {
      throw new RuntimeException("Can not find row " + rowId + " in partition " + partParam.getPartKey().getPartitionId());
    }

    PartitionGetResult result;
    if (row instanceof ServerLongDoubleRow) {
      result = new IndexPartGetDoubleResult(partParam.getPartKey(),
          ((ServerLongDoubleRow) row).get(indexes));
    } else if (row instanceof ServerLongFloatRow) {
      result = new IndexPartGetFloatResult(partParam.getPartKey(),
          ((ServerLongFloatRow) row).get(indexes));
    } else if (row instanceof ServerLongLongRow) {
      result = new IndexPartGetLongResult(partParam.getPartKey(),
          ((ServerLongLongRow) row).get(indexes));
    } else if (row instanceof ServerLongIntRow) {
      result = new IndexPartGetIntResult(partParam.getPartKey(),
          ((ServerLongIntRow) row).get(indexes));
    } else {
      throw new UnsupportedOperationException(
          "Index get use long type key not support " + row.getClass().getName() + "now");
    }

    LOG.debug("Partition get use time=" + (System.currentTimeMillis() - startTs) + " ms");
    return result;
  }

  /**
   * Merge all partition get result and return a sparse double vector
   *
   * @param partResults the partition results
   * @return a merged sparse double vector
   */

  @Override public GetResult merge(List<PartitionGetResult> partResults) {
    RowType rowType =
      PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.getMatrixId()).getRowType();

    switch (rowType) {
      case T_DOUBLE_SPARSE_LONGKEY:
        return new GetRowResult(ResponseType.SUCCESS,
          ValuesCombineUtils.mergeSparseDoubleVector((LongIndexGetParam) param, partResults));

      case T_DOUBLE_DENSE_LONGKEY_COMPONENT:
      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
        return new GetRowResult(ResponseType.SUCCESS,
          ValuesCombineUtils.mergeSparseDoubleCompVector((LongIndexGetParam) param, partResults));

      case T_FLOAT_SPARSE_LONGKEY:
        return new GetRowResult(ResponseType.SUCCESS,
          ValuesCombineUtils.mergeSparseFloatVector((LongIndexGetParam) param, partResults));

      case T_FLOAT_DENSE_LONGKEY_COMPONENT:
      case T_FLOAT_SPARSE_LONGKEY_COMPONENT:
        return new GetRowResult(ResponseType.SUCCESS,
          ValuesCombineUtils.mergeSparseFloatCompVector((LongIndexGetParam) param, partResults));

      case T_INT_SPARSE_LONGKEY:
        return new GetRowResult(ResponseType.SUCCESS,
          ValuesCombineUtils.mergeSparseIntVector((LongIndexGetParam) param, partResults));

      case T_INT_DENSE_LONGKEY_COMPONENT:
      case T_INT_SPARSE_LONGKEY_COMPONENT:
        return new GetRowResult(ResponseType.SUCCESS,
          ValuesCombineUtils.mergeSparseIntCompVector((LongIndexGetParam) param, partResults));

      case T_LONG_SPARSE_LONGKEY:
        return new GetRowResult(ResponseType.SUCCESS,
          ValuesCombineUtils.mergeSparseLongVector((LongIndexGetParam) param, partResults));

      case T_LONG_DENSE_LONGKEY_COMPONENT:
      case T_LONG_SPARSE_LONGKEY_COMPONENT:
        return new GetRowResult(ResponseType.SUCCESS,
          ValuesCombineUtils.mergeSparseLongCompVector((LongIndexGetParam) param, partResults));

      default:
        throw new UnsupportedOperationException(
          "Unsupport operation: update " + rowType + " to " + this.getClass().getName());
    }
  }
}
