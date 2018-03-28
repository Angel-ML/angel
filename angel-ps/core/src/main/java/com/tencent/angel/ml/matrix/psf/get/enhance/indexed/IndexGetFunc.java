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

import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ml.matrix.psf.get.single.GetRowResult;
import com.tencent.angel.ps.impl.matrix.*;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.ResponseType;
import com.tencent.angel.ml.matrix.RowType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

/**
 * Get the values of specifide index psfunc.
 */
public class IndexGetFunc extends GetFunc {
  private static final Log LOG = LogFactory.getLog(IndexGetFunc.class);

  public IndexGetFunc(IndexGetParam param) {
    super(param);
  }

  public IndexGetFunc( ) {
    this(null);
  }

  /**
   * Each server partition execute this function and return values of specified index.
   * @param partParam the partition parameter
   * @return values of specified index
   */
  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    long startTs = System.currentTimeMillis();
    ServerPartition part =
        psContext.getMatrixStorageManager()
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
        case T_DOUBLE_SPARSE_COMPONENT :{
          result = new IndexPartGetDoubleResult(partParam.getPartKey(), ((ServerDoubleRow) row).getValues(indexes));
          break;
        }

        case T_FLOAT_DENSE:
        case T_FLOAT_SPARSE:
        case T_FLOAT_SPARSE_COMPONENT: {
          result = new IndexPartGetFloatResult(partParam.getPartKey(), ((ServerFloatRow) row).getValues(indexes));
          break;
        }

        case T_INT_DENSE:
        case T_INT_SPARSE:
        case T_INT_SPARSE_COMPONENT: {
          result = new IndexPartGetIntResult(partParam.getPartKey(), ((ServerIntRow) row).getValues(indexes));
          break;
        }

        default:
          throw new UnsupportedOperationException("Unsupport operation: update " + rowType + " to " + this.getClass().getName());
      }
    }
    LOG.debug("Partition get use time=" + (System.currentTimeMillis() - startTs) + " ms");
    return result;
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    long startTs = System.currentTimeMillis();
    RowType rowType = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.getMatrixId()).getRowType();
    GetRowResult result = null;
    switch (rowType) {
      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE:
        result = new GetRowResult(ResponseType.SUCCESS, ValuesCombineUtils.mergeSparseDoubleVector((IndexGetParam) param, partResults));
        break;

      case T_DOUBLE_SPARSE_COMPONENT:
        result = new GetRowResult(ResponseType.SUCCESS, ValuesCombineUtils.mergeSparseDoubleCompVector((IndexGetParam) param, partResults));
        break;

      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE:
        result = new GetRowResult(ResponseType.SUCCESS, ValuesCombineUtils.mergeSparseFloatVector((IndexGetParam) param, partResults));
        break;

      case T_FLOAT_SPARSE_COMPONENT:
        result = new GetRowResult(ResponseType.SUCCESS, ValuesCombineUtils.mergeSparseFloatCompVector((IndexGetParam) param, partResults));
        break;

      case T_INT_DENSE:
      case T_INT_SPARSE:
        result = new GetRowResult(ResponseType.SUCCESS, ValuesCombineUtils.mergeSparseIntVector((IndexGetParam) param, partResults));
        break;

      case T_INT_SPARSE_COMPONENT:
        result = new GetRowResult(ResponseType.SUCCESS, ValuesCombineUtils.mergeSparseIntCompVector((IndexGetParam) param, partResults));
        break;

      default:
        throw new UnsupportedOperationException("Unsupport operation: update " + rowType + " to " + this.getClass().getName());
    }

    LOG.debug("Merge use time=" + (System.currentTimeMillis() - startTs) + " ms");
    return result;
  }
}
