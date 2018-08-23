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
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import com.tencent.angel.ps.storage.vector.*;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.ResponseType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

/**
 * Get the values of specifide index psfunc.
 */
public class IndexGet extends GetFunc {
  private static final Log LOG = LogFactory.getLog(IndexGet.class);

  public IndexGet(IndexGetParam param) {
    super(param);
  }

  public IndexGet() {
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
    ServerPartition part = psContext.getMatrixStorageManager()
      .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    PartitionGetResult result = null;
    if (part != null) {
      int rowId = ((IndexPartGetParam) partParam).getRowId();
      int[] indexes = ((IndexPartGetParam) partParam).getIndexes();

      ServerRow row = part.getRow(rowId);
      RowType rowType = row.getRowType();

      switch (rowType) {
        case T_DOUBLE_DENSE:
        case T_DOUBLE_SPARSE:
        case T_DOUBLE_DENSE_COMPONENT:
        case T_DOUBLE_SPARSE_COMPONENT: {
          result = new IndexPartGetDoubleResult(partParam.getPartKey(),
            ((ServerIntDoubleRow) row).get(indexes));
          break;
        }

        case T_FLOAT_DENSE:
        case T_FLOAT_SPARSE:
        case T_FLOAT_DENSE_COMPONENT:
        case T_FLOAT_SPARSE_COMPONENT: {
          result = new IndexPartGetFloatResult(partParam.getPartKey(),
            ((ServerIntFloatRow) row).get(indexes));
          break;
        }

        case T_INT_DENSE:
        case T_INT_SPARSE:
        case T_INT_DENSE_COMPONENT:
        case T_INT_SPARSE_COMPONENT: {
          result =
            new IndexPartGetIntResult(partParam.getPartKey(), ((ServerIntIntRow) row).get(indexes));
          break;
        }

        case T_LONG_DENSE:
        case T_LONG_SPARSE:
        case T_LONG_DENSE_COMPONENT:
        case T_LONG_SPARSE_COMPONENT: {
          result = new IndexPartGetLongResult(partParam.getPartKey(),
            ((ServerIntLongRow) row).get(indexes));
          break;
        }

        default:
          throw new UnsupportedOperationException(
            "Unsupport operation: update " + rowType + " to " + this.getClass().getName());
      }
    }
    LOG.debug("Partition get use time=" + (System.currentTimeMillis() - startTs) + " ms");
    return result;
  }

  @Override public GetResult merge(List<PartitionGetResult> partResults) {
    long startTs = System.currentTimeMillis();
    RowType rowType =
      PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.getMatrixId()).getRowType();
    GetRowResult result;
    switch (rowType) {
      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE:
        result = new GetRowResult(ResponseType.SUCCESS,
          ValuesCombineUtils.mergeSparseDoubleVector((IndexGetParam) param, partResults));
        break;

      case T_DOUBLE_SPARSE_COMPONENT:
      case T_DOUBLE_DENSE_COMPONENT:
        result = new GetRowResult(ResponseType.SUCCESS,
          ValuesCombineUtils.mergeSparseDoubleCompVector((IndexGetParam) param, partResults));
        break;

      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE:
        result = new GetRowResult(ResponseType.SUCCESS,
          ValuesCombineUtils.mergeSparseFloatVector((IndexGetParam) param, partResults));
        break;

      case T_FLOAT_SPARSE_COMPONENT:
      case T_FLOAT_DENSE_COMPONENT:
        result = new GetRowResult(ResponseType.SUCCESS,
          ValuesCombineUtils.mergeSparseFloatCompVector((IndexGetParam) param, partResults));
        break;

      case T_INT_DENSE:
      case T_INT_SPARSE:
        result = new GetRowResult(ResponseType.SUCCESS,
          ValuesCombineUtils.mergeSparseIntVector((IndexGetParam) param, partResults));
        break;

      case T_INT_DENSE_COMPONENT:
      case T_INT_SPARSE_COMPONENT:
        result = new GetRowResult(ResponseType.SUCCESS,
          ValuesCombineUtils.mergeSparseIntCompVector((IndexGetParam) param, partResults));
        break;

      case T_LONG_DENSE:
      case T_LONG_SPARSE:
        result = new GetRowResult(ResponseType.SUCCESS,
          ValuesCombineUtils.mergeSparseLongVector((IndexGetParam) param, partResults));
        break;

      case T_LONG_DENSE_COMPONENT:
      case T_LONG_SPARSE_COMPONENT:
        result = new GetRowResult(ResponseType.SUCCESS,
          ValuesCombineUtils.mergeSparseLongCompVector((IndexGetParam) param, partResults));
        break;


      default:
        throw new UnsupportedOperationException(
          "Unsupport operation: update " + rowType + " to " + this.getClass().getName());
    }

    return result;
  }
}
