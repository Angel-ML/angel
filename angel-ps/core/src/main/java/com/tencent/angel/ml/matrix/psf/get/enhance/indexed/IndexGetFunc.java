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

import com.tencent.angel.ml.math.vector.SparseDoubleVector;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ml.matrix.psf.get.single.GetRowResult;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.*;
import com.tencent.angel.psagent.matrix.ResponseType;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import java.nio.DoubleBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Get the values of specifide index psfunc.
 */
public class IndexGetFunc extends GetFunc {

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
    ServerPartition part =
        PSContext.get().getMatrixPartitionManager()
            .getPartition(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      int rowId = ((IndexPartGetParam) partParam).getRowId();
      int[] index = ((IndexPartGetParam) partParam).getIndex();
      int paramId = ((IndexPartGetParam) partParam).getParamId();

      ServerRow row = part.getRow(rowId);
      MLProtos.RowType rowType = row.getRowType();
      double[] values = new double[index.length];
      switch (rowType) {
        case T_DOUBLE_DENSE: {
          values = getVluesofServerDenseDoubleRow(row, index);
          return new IndexGetResult(paramId, values);
        }

        case T_DOUBLE_SPARSE: {
          values = getVluesofServerSparseDoubleRow(row, index);
          return new IndexGetResult(paramId, values);
        }

        case T_DOUBLE_SPARSE_LONGKEY: {
          values = getVluesofServerSparseDoubleLongRow(row, index);
          return new IndexGetResult(paramId, values);
        }

        default:
          throw new UnsupportedOperationException("Unsupport operation: update " + rowType + " to " + this.getClass().getName());
      }
    }
    return null;
  }

  /**
   * Get values of specified index in a server row of a server partition.
   * @param row server row of a partition
   * @param index specified index
   * @return values of specified index
   */
  private double[] getVluesofServerDenseDoubleRow(ServerRow row, int[] index) {
    int startCol = (int) row.getStartCol();
    DoubleBuffer data = ((ServerDenseDoubleRow) row).getData();
    double[] values = new double[index.length];

    for (int i = 0; i < index.length; i++) {
      values[i] = data.get(index[i] - startCol);
    }

    return values;
  }

  private double[] getVluesofServerSparseDoubleRow(ServerRow row, int[] index) {
    Int2DoubleOpenHashMap data = ((ServerSparseDoubleRow) row).getData();
    double[] values = new double[index.length];

    for (int i = 0; i < index.length; i++) {
      values[i] = data.get(index[i]);
    }

    return values;
  }

  private double[] getVluesofServerSparseDoubleLongRow(ServerRow row, int[] index) {
    Long2DoubleOpenHashMap data = ((ServerSparseDoubleLongKeyRow) row).getData();
    double[] values = new double[index.length];

    for (int i = 0; i < index.length; i++) {
      values[i] = data.get(index[i]);
    }

    return values;
  }


  /**
   * Merge all partition get result and return a sparse double vector
   * @param partResults the partition results
   * @return a merged sparse double vector
   */

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    GetParam getParam = getParam();
    List<PartitionGetParam> partParams = getParam.split();
    Map<Integer, PartitionGetParam> paramIdToPartGetParam = new HashMap<Integer,
        PartitionGetParam>();
    for (PartitionGetParam partParam: partParams) {
      paramIdToPartGetParam.put(((IndexPartGetParam) partParam).getParamId(), partParam);
    }

    MLProtos.RowType rowType = PSContext.get().getMatrixPartitionManager()
          .getPartition(param.getMatrixId(), ((IndexGetParam) param).getRowId())
          .getRow(((IndexGetParam) param).getRowId()).getRowType();

    switch (rowType) {
      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE:
        return mergeSparseDoubleVector(paramIdToPartGetParam, partResults);

      default:
        throw new UnsupportedOperationException("Unsupport operation: update " + rowType + " to " + this.getClass().getName());
    }

  }

  public GetResult mergeSparseDoubleVector(Map<Integer, PartitionGetParam> paramIdToPartGetParam,
                                           List<PartitionGetResult> partResults) {

    SparseDoubleVector vector = new SparseDoubleVector();
    for (PartitionGetResult part: partResults) {
      int paramId = ((IndexGetResult) part).getParamId();
      int[] index = ((IndexPartGetParam) paramIdToPartGetParam.get(paramId)).getIndex();
      double[] values = ((IndexGetResult) part).getValues();
      for (int i = 0; i < index.length; i++) {
        vector.set(index[i], values[i]);
      }
    }

    return new GetRowResult(ResponseType.SUCCESS, vector);
  }


}
