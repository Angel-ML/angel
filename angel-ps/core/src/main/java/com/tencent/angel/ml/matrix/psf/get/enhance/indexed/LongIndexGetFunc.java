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
import com.tencent.angel.ml.math.vector.CompSparseLongKeyDoubleVector;
import com.tencent.angel.ml.math.vector.SparseLongKeyDoubleVector;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ml.matrix.psf.get.single.GetRowResult;
import com.tencent.angel.ps.impl.matrix.*;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.ResponseType;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
      long[] index = ((LongIndexPartGetParam) partParam).getIndex();
      PartitionKey partKey = ((LongIndexPartGetParam) partParam).getPartKey();

      ServerRow row = part.getRow(rowId);
      RowType rowType = row.getRowType();
      double[] values;
      switch (rowType) {
        case T_DOUBLE_SPARSE_LONGKEY:
        case T_DOUBLE_SPARSE_LONGKEY_COMPONENT: {
          values = getVluesofServerSparseDoubleLongRow(row, index);
          return new LongIndexGetResult(partKey, values);
        }

        default:
          throw new UnsupportedOperationException("Unsupport operation: update " + rowType + " to " + this.getClass().getName());
      }
    }
    return null;
  }

  private double[] getVluesofServerSparseDoubleLongRow(ServerRow row, long[] index) {
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
    RowType rowType = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.getMatrixId()).getRowType();

    switch (rowType) {
      case T_DOUBLE_SPARSE_LONGKEY:
        return mergeSparseDoubleVector(((LongIndexGetParam) param).getPartKeyToIndexesMap(), partResults);

      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
        return mergeSparseDoubleCompVector(((LongIndexGetParam) param).getPartKeyToIndexesMap(), partResults);

      default:
        throw new UnsupportedOperationException("Unsupport operation: update " + rowType + " to " + this.getClass().getName());
    }
  }

  private GetResult mergeSparseDoubleVector(Map<PartitionKey, long[]> partKeyToIndexesMap,
    List<PartitionGetResult> partResults) {
    SparseLongKeyDoubleVector vector = new SparseLongKeyDoubleVector(
      PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(getParam().getMatrixId()).getColNum(),
      ((LongIndexGetParam) param).size());

    for (PartitionGetResult part: partResults) {
      PartitionKey partKey = ((LongIndexGetResult) part).partKey;
      long[] indexes = partKeyToIndexesMap.get(partKey);
      double[] values = ((LongIndexGetResult) part).getValues();
      for (int i = 0; i < indexes.length; i++) {
        vector.set(indexes[i], values[i]);
      }
    }
    vector.setMatrixId(param.getMatrixId());
    vector.setRowId(((LongIndexGetParam) param).getRowId());
    return new GetRowResult(ResponseType.SUCCESS, vector);
  }

  private GetResult mergeSparseDoubleCompVector(Map<PartitionKey, long[]> partKeyToIndexesMap,
    List<PartitionGetResult> partResults) {
    int size = partResults.size();
    Map<PartitionKey, PartitionGetResult> partKeyToResultMap = new HashMap<>(size);
    for(int i = 0; i < size; i++) {
      partKeyToResultMap.put(((LongIndexGetResult)partResults.get(i)).getPartKey(), partResults.get(i));
    }

    List<PartitionKey> partKeys = PSAgentContext.get().getMatrixMetaManager().getPartitions(
      param.matrixId, ((LongIndexGetParam) param).getRowId());
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.matrixId);

    partKeys.sort((PartitionKey part1, PartitionKey part2) -> {
      return (int)(part1.getStartCol() - part2.getStartCol());
    });

    size = partKeys.size();
    SparseLongKeyDoubleVector [] splitVecs = new SparseLongKeyDoubleVector[size];
    for(int i = 0; i < size; i++) {
      if(partKeyToIndexesMap.containsKey(partKeys.get(i))) {
        splitVecs[i] = new SparseLongKeyDoubleVector(meta.getColNum(),
          partKeyToIndexesMap.get(partKeys.get(i)),
          ((LongIndexGetResult)partKeyToResultMap.get(partKeys.get(i))).getValues());
      }
    }

    CompSparseLongKeyDoubleVector vector = new CompSparseLongKeyDoubleVector(meta.getId(),
      ((LongIndexGetParam) param).getRowId(), meta.getColNum(), partKeys.toArray(new PartitionKey[0]), splitVecs);
    return new GetRowResult(ResponseType.SUCCESS, vector);
  }
}
