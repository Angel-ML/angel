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
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerIntDoubleRow;
import com.tencent.angel.ps.storage.vector.ServerIntFloatRow;
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

    //    System.out.print("PartitionGet ");
    //    for (int i = 0; i < cols.length; i ++) {
    //      System.out.print(cols[i] + " ");
    //    }
    //    System.out.println();

    Vector result;
    switch (partition.getRowType()) {
      case T_DOUBLE_DENSE:
        ServerIntDoubleRow[] doubles = new ServerIntDoubleRow[rows.length];
        for (int i = 0; i < rows.length; i++)
          doubles[i] = (ServerIntDoubleRow) partition.getRow(rows[i]);
        result = doGet(doubles, cols);
        break;
      case T_FLOAT_DENSE:
        ServerIntFloatRow[] floats = new ServerIntFloatRow[rows.length];
        for (int i = 0; i < rows.length; i++)
          floats[i] = (ServerIntFloatRow) partition.getRow(rows[i]);
        result = doGet(floats, cols);
        break;
      default:
        throw new AngelException("Model type should be dense and float!");
    }


    return new PartitionGetColsResult(rows, cols, result);
  }

  private Vector doGet(ServerIntDoubleRow[] rows, long[] cols) {
    //    System.out.println("doGet Double cols.length=" + cols.length);

    IntDoubleVector[] vectors = new IntDoubleVector[cols.length];
    for (int i = 0; i < cols.length; i++) {
      vectors[i] = VFactory.denseDoubleVector(rows.length);
      for (int j = 0; j < rows.length; j++)
        vectors[i].set(j, rows[j].get((int) cols[i]));
    }

    return VFactory.compIntDoubleVector(cols.length, vectors, rows.length);
  }

  private Vector doGet(ServerIntFloatRow[] rows, long[] cols) {
    IntFloatVector[] vectors = new IntFloatVector[cols.length];
    for (int i = 0; i < cols.length; i++) {
      vectors[i] = VFactory.denseFloatVector(rows.length);
      for (int j = 0; j < rows.length; j++)
        vectors[i].set(j, rows[j].get((int) cols[i]));
    }

    return VFactory.compIntFloatVector(cols.length, vectors, rows.length);
  }

  @Override public GetResult merge(List<PartitionGetResult> partResults) {

    PartitionGetColsResult rr = (PartitionGetColsResult) partResults.get(0);
    // LOG.info("Here merge");
    if (rr.vector instanceof CompIntDoubleVector) {
      Map<Long, Vector> maps = new HashMap<>();
      //       int sum = 0;
      for (PartitionGetResult r : partResults) {
        PartitionGetColsResult rrr = (PartitionGetColsResult) r;
        long[] cols = rrr.cols;
        //         sum += rrr.cols.length;
        CompIntDoubleVector vector = (CompIntDoubleVector) rrr.vector;
        for (int i = 0; i < cols.length; i++)
          maps.put(cols[i], vector.getPartitions()[i]);
      }

      //       LOG.error("double rrr.cols = " + sum + ", map size = " + maps.size());


      //      IntDoubleVector[] allVectors = new IntDoubleVector[allColumns.length];
      //      for (int i = 0; i < allColumns.length; i ++)
      //        allVectors[i] = maps.get(allColumns[i]);
      return new GetColsResult(maps);

      //      return new GetColsResult(VFactory.denseLongVector(allColumns),
      //              VFactory.compIntDoubleVector(dim, allVectors, allVectors.length));

    } else if (rr.vector instanceof CompIntFloatVector) {
      Map<Long, Vector> maps = new HashMap<>();
      //       int sum = 0;
      for (PartitionGetResult r : partResults) {
        PartitionGetColsResult rrr = (PartitionGetColsResult) r;
        long[] cols = rrr.cols;
        //         sum += cols.length;
        CompIntFloatVector vector = (CompIntFloatVector) rrr.vector;
        for (int i = 0; i < cols.length; i++) {
          maps.put(cols[i], vector.getPartitions()[i]);
          //          System.out.print(" " + cols[i]);
        }
        //        System.out.println();
      }

      //       LOG.error("float rrr.cols = " + sum + ", map size = " + maps.size());

      return new GetColsResult(maps);
      //      IntFloatVector[] allVectors = new IntFloatVector[allColumns.length];
      //      for (int i = 0; i < allColumns.length; i ++)
      //        allVectors[i] = maps.get(allColumns[i]);

      //      return new GetColsResult(VFactory.denseLongVector(allColumns),
      //              VFactory.compIntFloatVector(dim, allVectors, allVectors.length));
    } else {
      throw new AngelException("Data type should be double or float!");
    }
  }

}
