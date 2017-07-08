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
 */

package com.tencent.angel.psagent.matrix.transport.adapter;

import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.*;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ps.impl.matrix.*;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.protobuf.generated.MLProtos;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Row splits combine tool.
 */
public class RowSplitCombineUtils {
  private static final Log LOG = LogFactory.getLog(RowSplitCombineUtils.class);

  /**
   * Combine the row splits use pipeline mode.
   * 
   * @param cache the result cache for GET_ROW sub-requests
   * @param matrixId matrix id
   * @param rowIndex row index
   * @return TVector merged row
   * @throws InterruptedException interrupted while waiting for row splits
   */
  public static TVector combineRowSplitsPipeline(GetRowPipelineCache cache, int matrixId,
      int rowIndex) throws InterruptedException {
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    MLProtos.RowType rowType = matrixMeta.getRowType();

    switch (rowType) {
      case T_DOUBLE_DENSE:
        return combineDenseDoubleRowSplits(cache, matrixMeta, rowIndex);

      case T_DOUBLE_SPARSE: {
        int splitNum = cache.getTotalRequestNum();
        List<ServerRow> splits = new ArrayList<ServerRow>(splitNum);
        ServerRow split = null;

        int index = 0;
        while ((split = cache.poll()) != null && index < splitNum) {
          splits.add(split);
          index++;
        }

        return combineServerSparseDoubleRowSplits(splits, matrixMeta, rowIndex);
      }

      case T_FLOAT_SPARSE: {
        int splitNum = cache.getTotalRequestNum();
        List<ServerRow> splits = new ArrayList<ServerRow>(splitNum);
        ServerRow split = null;

        int index = 0;
        while ((split = cache.poll()) != null && index < splitNum) {
          splits.add(split);
          index++;
        }

        return combineServerSparseFloatRowSplits(splits, matrixMeta, rowIndex);
      }

      case T_INT_DENSE:
        return combineDenseIntRowSplits(cache, matrixMeta, rowIndex);

      case T_FLOAT_DENSE: {
        return combineDenseFloatRowSplits(cache, matrixMeta, rowIndex);
      }
      case T_INT_SPARSE: {
        int splitNum = cache.getTotalRequestNum();
        List<ServerRow> splits = new ArrayList<ServerRow>(splitNum);
        ServerRow split = null;

        int index = 0;
        while ((split = cache.poll()) != null && index < splitNum) {
          splits.add(split);
          index++;
        }

        return combineServerSparseIntRowSplits(splits, matrixMeta, rowIndex);
      }

      default:
        return null;
    }
  }


  private static TVector combineDenseIntRowSplits(GetRowPipelineCache pipelineCache,
      MatrixMeta matrixMeta, int rowIndex) throws InterruptedException {
    int colNum = matrixMeta.getColNum();
    int[] dataArray = new int[colNum];

    int clock = Integer.MAX_VALUE;
    while (true) {
      ServerRow split = pipelineCache.poll();
      if (split == null) {
        TVector row = new DenseIntVector(colNum, dataArray);
        row.setMatrixId(matrixMeta.getId());
        row.setRowId(rowIndex);
        row.setClock(clock);
        return row;
      }

      if (split.getClock() < clock) {
        clock = split.getClock();
      }
      ((ServerDenseIntRow) split).mergeTo(dataArray);
    }
  }

  private static TVector combineDenseFloatRowSplits(GetRowPipelineCache pipelineCache,
      MatrixMeta matrixMeta, int rowIndex) throws InterruptedException {
    int colNum = matrixMeta.getColNum();
    float[] dataArray = new float[colNum];

    int clock = Integer.MAX_VALUE;
    while (true) {
      ServerRow split = pipelineCache.poll();
      if (split == null) {
        LOG.info("split is null.");
        TVector row = new DenseFloatVector(colNum, dataArray);
        row.setMatrixId(matrixMeta.getId());
        row.setRowId(rowIndex);
        row.setClock(clock);
        return row;
      }

      if (split.getClock() < clock) {
        clock = split.getClock();
      }
      ((ServerDenseFloatRow) split).mergeTo(dataArray);
    }
  }

  private static TVector combineDenseDoubleRowSplits(GetRowPipelineCache pipelineCache,
      MatrixMeta matrixMeta, int rowIndex) throws InterruptedException {
    int colNum = matrixMeta.getColNum();
    double[] dataArray = new double[colNum];
    int clock = Integer.MAX_VALUE;

    while (true) {
      ServerRow split = pipelineCache.poll();
      if (split == null) {
        TVector row = new DenseDoubleVector(colNum, dataArray);
        row.setClock(clock);
        row.setMatrixId(matrixMeta.getId());
        row.setRowId(rowIndex);
        return row;
      }

      if (split.getClock() < clock) {
        clock = split.getClock();
      }
      ((ServerDenseDoubleRow) split).mergeTo(dataArray);
    }
  }

  /**
   * Combine row splits of a single matrix row.
   * 
   * @param rowSplits row splits
   * @param matrixId matrix id
   * @param rowIndex row index
   * @return TVector merged row
   */
  public static TVector combineServerRowSplits(List<ServerRow> rowSplits, int matrixId, int rowIndex) {
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    MLProtos.RowType rowType = matrixMeta.getRowType();

    switch (rowType) {
      case T_DOUBLE_DENSE:
        return combineServerDenseDoubleRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_FLOAT_DENSE:
        return combineServerDenseFloatRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_DOUBLE_SPARSE:
        return combineServerSparseDoubleRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_FLOAT_SPARSE:
        return combineServerSparseFloatRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_INT_DENSE:
        return combineServerDenseIntRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_INT_SPARSE:
        return combineServerSparseIntRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_INT_ARBITRARY:
        return combineServerArbitaryIntRowSplits(rowSplits, matrixMeta, rowIndex);

      default:
        LOG.error("receive a row with row_type " + rowType);
        return null;
    }
  }

  private static TVector combineServerArbitaryIntRowSplits(List<ServerRow> rowSplits,
      MatrixMeta matrixMeta, int rowIndex) {
    // TODO Auto-generated method stub
    return null;
  }

  private static TVector combineServerSparseIntRowSplits(List<ServerRow> rowSplits,
      MatrixMeta matrixMeta, int rowIndex) {
    int colNum = matrixMeta.getColNum();
    int splitNum = rowSplits.size();
    int totalElemNum = 0;
    int[] lens = new int[splitNum];

    Collections.sort(rowSplits, new Comparator<ServerRow>() {
      @Override
      public int compare(ServerRow r1, ServerRow r2) {
        return r1.getStartCol() - r2.getStartCol();
      }
    });

    int elemNum = 0;
    for (int i = 0; i < splitNum; i++) {
      elemNum = rowSplits.get(i).size();
      totalElemNum += elemNum;
      lens[i] = elemNum;
    }

    int[] indexes = new int[totalElemNum];
    int[] values = new int[totalElemNum];

    int clock = Integer.MAX_VALUE;
    int startPos = 0;
    for (int i = 0; i < splitNum; i++) {
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
      ((ServerSparseIntRow) rowSplits.get(i)).mergeTo(indexes, values, startPos, lens[i]);
      startPos += lens[i];
    }

    TVector row = new SparseIntVector(colNum, indexes, values);
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);
    row.setClock(clock);
    return row;

  }

  private static TVector combineServerDenseIntRowSplits(List<ServerRow> rowSplits,
      MatrixMeta matrixMeta, int rowIndex) {
    int colNum = matrixMeta.getColNum();
    int[] dataArray = new int[colNum];

    Collections.sort(rowSplits, new Comparator<ServerRow>() {
      @Override
      public int compare(ServerRow r1, ServerRow r2) {
        return r1.getStartCol() - r2.getStartCol();
      }
    });

    int clock = Integer.MAX_VALUE;
    int size = rowSplits.size();
    for (int i = 0; i < size; i++) {
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
      ((ServerDenseIntRow) rowSplits.get(i)).mergeTo(dataArray);
    }

    TVector row = new DenseIntVector(colNum, dataArray);
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);
    row.setClock(clock);
    return row;
  }

  private static TVector combineServerSparseDoubleRowSplits(List<ServerRow> rowSplits,
      MatrixMeta matrixMeta, int rowIndex) {
    int colNum = matrixMeta.getColNum();
    int splitNum = rowSplits.size();
    int totalElemNum = 0;
    int[] lens = new int[splitNum];

    Collections.sort(rowSplits, new Comparator<ServerRow>() {
      @Override
      public int compare(ServerRow r1, ServerRow r2) {
        return r1.getStartCol() - r2.getStartCol();
      }
    });

    int elemNum = 0;
    for (int i = 0; i < splitNum; i++) {
      elemNum = rowSplits.get(i).size();
      totalElemNum += elemNum;
      lens[i] = elemNum;
    }

    int[] indexes = new int[totalElemNum];
    double[] values = new double[totalElemNum];

    int clock = Integer.MAX_VALUE;
    int startPos = 0;
    for (int i = 0; i < splitNum; i++) {
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
      ((ServerSparseDoubleRow) rowSplits.get(i)).mergeTo(indexes, values, startPos, lens[i]);
      startPos += lens[i];
    }

    TVector row = new SparseDoubleVector(colNum, indexes, values);
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);
    row.setClock(clock);
    return row;

  }

  private static TVector combineServerSparseFloatRowSplits(List<ServerRow> rowSplits,
      MatrixMeta matrixMeta, int rowIndex) {
    int colNum = matrixMeta.getColNum();
    int splitNum = rowSplits.size();
    int totalElemNum = 0;
    int[] lens = new int[splitNum];

    Collections.sort(rowSplits, new Comparator<ServerRow>() {
      @Override
      public int compare(ServerRow r1, ServerRow r2) {
        return r1.getStartCol() - r2.getStartCol();
      }
    });

    int elemNum = 0;
    for (int i = 0; i < splitNum; i++) {
      elemNum = rowSplits.get(i).size();
      totalElemNum += elemNum;
      lens[i] = elemNum;
    }

    int[] indexes = new int[totalElemNum];
    float[] values = new float[totalElemNum];

    int clock = Integer.MAX_VALUE;
    int startPos = 0;
    for (int i = 0; i < splitNum; i++) {
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
      ((ServerSparseFloatRow) rowSplits.get(i)).mergeTo(indexes, values, startPos, lens[i]);
      startPos += lens[i];
    }

    TVector row = new SparseFloatVector(colNum, indexes, values);
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);
    row.setClock(clock);
    return row;

  }


  private static TVector combineServerDenseDoubleRowSplits(List<ServerRow> rowSplits,
      MatrixMeta matrixMeta, int rowIndex) {
    int colNum = matrixMeta.getColNum();
    double[] dataArray = new double[colNum];

    Collections.sort(rowSplits, new Comparator<ServerRow>() {
      @Override
      public int compare(ServerRow r1, ServerRow r2) {
        return r1.getStartCol() - r2.getStartCol();
      }
    });

    int clock = Integer.MAX_VALUE;
    int size = rowSplits.size();
    for (int i = 0; i < size; i++) {
      if(rowSplits.get(i) == null) {
        continue;
      }
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
      ((ServerDenseDoubleRow) rowSplits.get(i)).mergeTo(dataArray);
    }

    TVector row = new DenseDoubleVector(colNum, dataArray);
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);
    row.setClock(clock);
    return row;
  }

  private static TVector combineServerDenseFloatRowSplits(List<ServerRow> rowSplits,
      MatrixMeta matrixMeta, int rowIndex) {
    int colNum = matrixMeta.getColNum();
    float[] dataArray = new float[colNum];

    Collections.sort(rowSplits, new Comparator<ServerRow>() {
      @Override
      public int compare(ServerRow r1, ServerRow r2) {
        return r1.getStartCol() - r2.getStartCol();
      }
    });

    int clock = Integer.MAX_VALUE;
    int size = rowSplits.size();
    for (int i = 0; i < size; i++) {
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
      ((ServerDenseFloatRow) rowSplits.get(i)).mergeTo(dataArray);
    }

    TVector row = new DenseFloatVector(colNum, dataArray);
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);
    row.setClock(clock);
    return row;
  }
}
