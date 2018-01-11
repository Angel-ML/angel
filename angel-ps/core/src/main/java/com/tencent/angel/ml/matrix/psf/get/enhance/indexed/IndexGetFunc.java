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
import com.tencent.angel.ml.math.vector.CompSparseDoubleVector;
import com.tencent.angel.ml.math.vector.SparseDoubleVector;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ml.matrix.psf.get.single.GetRowResult;
import com.tencent.angel.ps.impl.matrix.*;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.ResponseType;
import com.tencent.angel.ml.matrix.RowType;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;

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
        psContext.getMatrixStorageManager()
            .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      int rowId = ((IndexPartGetParam) partParam).getRowId();
      int[] indexes = ((IndexPartGetParam) partParam).getIndexes();

      ServerRow row = part.getRow(rowId);
      RowType rowType = row.getRowType();
      double[] values;
      switch (rowType) {
        case T_DOUBLE_DENSE: {
          values = getVluesofServerDenseDoubleRow(row, indexes);
          return new IndexGetResult(partParam.getPartKey(), values);
        }

        case T_DOUBLE_SPARSE:
        case T_DOUBLE_SPARSE_COMPONENT:{
          values = getVluesofServerSparseDoubleRow(row, indexes);
          return new IndexGetResult(partParam.getPartKey(), values);
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

  /**
   * Merge all partition get result and return a sparse double vector
   * @param partResults the partition results
   * @return a merged sparse double vector
   */

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    RowType rowType = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.getMatrixId()).getRowType();
    switch (rowType) {
      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE:
        return mergeSparseDoubleVector(((IndexGetParam) param).getPartKeyToIndexesMap(), partResults);

      case T_DOUBLE_SPARSE_COMPONENT:
        return mergeSparseDoubleCompVector(((IndexGetParam) param).getPartKeyToIndexesMap(), partResults);

      default:
        throw new UnsupportedOperationException("Unsupport operation: update " + rowType + " to " + this.getClass().getName());
    }

  }

  private GetResult mergeSparseDoubleVector(Map<PartitionKey, int[]> partKeyToIndexesMap,
    List<PartitionGetResult> partResults) {
    SparseDoubleVector vector = new SparseDoubleVector(
      (int)PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(getParam().getMatrixId()).getColNum(),
      ((IndexGetParam) param).size());

    for (PartitionGetResult part: partResults) {
      PartitionKey partKey = ((IndexGetResult) part).partKey;
      int[] indexes = partKeyToIndexesMap.get(partKey);
      double[] values = ((IndexGetResult) part).getValues();
      for (int i = 0; i < indexes.length; i++) {
        vector.set(indexes[i], values[i]);
      }
    }
    vector.setMatrixId(param.getMatrixId());
    vector.setRowId(((IndexGetParam) param).getRowId());
    return new GetRowResult(ResponseType.SUCCESS, vector);
  }

  private GetResult mergeSparseDoubleCompVector(Map<PartitionKey, int[]> partKeyToIndexesMap,
    List<PartitionGetResult> partResults) {
    int size = partResults.size();
    Map<PartitionKey, PartitionGetResult> partKeyToResultMap = new HashMap<>(size);
    for(int i = 0; i < size; i++) {
      partKeyToResultMap.put(((IndexGetResult)partResults.get(i)).getPartKey(), partResults.get(i));
    }

    List<PartitionKey> partKeys = PSAgentContext.get().getMatrixMetaManager().getPartitions(
      param.matrixId, ((IndexGetParam) param).getRowId());
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(param.matrixId);

    partKeys.sort((PartitionKey part1, PartitionKey part2) -> {
      return (int)(part1.getStartCol() - part2.getStartCol());
    });

    size = partKeys.size();
    SparseDoubleVector [] splitVecs = new SparseDoubleVector[size];
    for(int i = 0; i < size; i++) {
      if(partKeyToIndexesMap.containsKey(partKeys.get(i))) {
        splitVecs[i] = new SparseDoubleVector((int)meta.getColNum(),
          partKeyToIndexesMap.get(partKeys.get(i)),
          ((IndexGetResult)partKeyToResultMap.get(partKeys.get(i))).getValues());
      }
    }

    CompSparseDoubleVector vector = new CompSparseDoubleVector(meta.getId(),
      ((IndexGetParam) param).getRowId(), (int)meta.getColNum(), partKeys.toArray(new PartitionKey[0]), splitVecs);
    return new GetRowResult(ResponseType.SUCCESS, vector);
  }
}
