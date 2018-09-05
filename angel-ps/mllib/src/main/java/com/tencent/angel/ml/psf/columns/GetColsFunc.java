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


package com.tencent.angel.ml.psf.columns;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.vector.*;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetColsFunc extends GetFunc {
  Log LOG = LogFactory.getLog(GetColsFunc.class);

  public GetColsFunc(GetColsParam param) {
    super(param);
  }

  public GetColsFunc() {
    super(null);
  }

  @Override public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartitionGetColsParam param = (PartitionGetColsParam) partParam;
    int[] rows = param.rows;
    long[] cols = param.cols;
    int matId = param.getMatrixId();
    int partitionId = param.getPartKey().getPartitionId();
    Arrays.sort(rows);

    ServerPartition partition = psContext.getMatrixStorageManager().getPart(matId, partitionId);
    Vector[] splits = new Vector[rows.length];
    for (int i = 0; i < rows.length; i++) {
      splits[i] = partition.getRow(rows[i]).getSplit();
    }
    Vector result = doGet(splits, cols, partition.getRowType());

    return new PartitionGetColsResult(rows, cols, result);
  }

  private Vector doGet(Vector[] rows, long[] cols, RowType rowType) {
    switch (rowType) {
      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE: {
        IntDoubleVector[] vectors = new IntDoubleVector[cols.length];

        for (int i = 0; i < cols.length; i++) {
          vectors[i] = VFactory.denseDoubleVector(rows.length);
          for (int j = 0; j < rows.length; j++)
            vectors[i].set(j, ((IntDoubleVector) rows[j]).get((int) cols[i]));
        }
        return VFactory.compIntDoubleVector(cols.length, vectors, rows.length);
      }

      case T_DOUBLE_SPARSE_LONGKEY: {
        IntDoubleVector[] vectors = new IntDoubleVector[cols.length];
        for (int i = 0; i < cols.length; i++) {
          vectors[i] = VFactory.denseDoubleVector(rows.length);
          for (int j = 0; j < rows.length; j++)
            vectors[i].set(j, ((LongDoubleVector) rows[j]).get(cols[i]));
        }
        return VFactory.compIntDoubleVector(cols.length, vectors, rows.length);
      }

      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE: {
        IntFloatVector[] vectors = new IntFloatVector[cols.length];
        for (int i = 0; i < cols.length; i++) {
          vectors[i] = VFactory.denseFloatVector(rows.length);
          for (int j = 0; j < rows.length; j++)
            vectors[i].set(j, ((IntFloatVector) rows[j]).get((int) cols[i]));
        }
        return VFactory.compIntFloatVector(cols.length, vectors, rows.length);
      }

      case T_FLOAT_SPARSE_LONGKEY: {
        IntFloatVector[] vectors = new IntFloatVector[cols.length];
        for (int i = 0; i < cols.length; i++) {
          vectors[i] = VFactory.denseFloatVector(rows.length);
          for (int j = 0; j < rows.length; j++)
            vectors[i].set(j, ((LongFloatVector) rows[j]).get(cols[i]));
        }
        return VFactory.compIntFloatVector(cols.length, vectors, rows.length);
      }

      default:
        throw new AngelException("The rowType is not support!");
    }
  }

  @Override public GetResult merge(List<PartitionGetResult> partResults) {
    PartitionGetColsResult rr = (PartitionGetColsResult) partResults.get(0);
    if (rr.vector instanceof CompIntDoubleVector) {
      Map<Long, Vector> maps = new HashMap<>();
      for (PartitionGetResult r : partResults) {
        PartitionGetColsResult rrr = (PartitionGetColsResult) r;
        long[] cols = rrr.cols;
        CompIntDoubleVector vector = (CompIntDoubleVector) rrr.vector;
        for (int i = 0; i < cols.length; i++)
          maps.put(cols[i], vector.getPartitions()[i]);
      }
      return new GetColsResult(maps);
    } else if (rr.vector instanceof CompIntFloatVector) {
      Map<Long, Vector> maps = new HashMap<>();
      for (PartitionGetResult r : partResults) {
        PartitionGetColsResult rrr = (PartitionGetColsResult) r;
        long[] cols = rrr.cols;
        CompIntFloatVector vector = (CompIntFloatVector) rrr.vector;
        for (int i = 0; i < cols.length; i++) {
          maps.put(cols[i], vector.getPartitions()[i]);
        }
      }
      return new GetColsResult(maps);
    } else {
      throw new AngelException("Data type should be double or float!");
    }
  }

}
