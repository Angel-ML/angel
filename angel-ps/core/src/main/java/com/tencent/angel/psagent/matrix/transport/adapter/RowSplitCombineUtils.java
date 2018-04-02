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

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.*;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ps.impl.matrix.*;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.psagent.PSAgentContext;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
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
  private static final Comparator serverRowComp = new StartColComparator();
  private static final Comparator partKeyComp = new PartitionKeyComparator();

  static class StartColComparator implements Comparator<ServerRow> {

    @Override public int compare(ServerRow r1, ServerRow r2) {
      return compareStartCol(r1, r2);
    }

    private int compareStartCol(ServerRow r1, ServerRow r2) {
      if(r1.getStartCol() > r2.getStartCol()) {
        return 1;
      } else if(r1.getStartCol() < r2.getStartCol()) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  static class PartitionKeyComparator implements Comparator<PartitionKey> {

    @Override public int compare(PartitionKey p1, PartitionKey p2) {
      return comparePartitionKey(p1, p2);
    }

    private int comparePartitionKey(PartitionKey p1, PartitionKey p2) {
      if(p1.getStartCol() > p2.getStartCol()) {
        return 1;
      } else if(p1.getStartCol() < p2.getStartCol()) {
        return -1;
      } else {
        return 0;
      }
    }
  }

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
                                                 int rowIndex) throws Exception {
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    RowType rowType = matrixMeta.getRowType();

    switch (rowType) {
      case T_DOUBLE_DENSE:
        return combineDenseDoubleRowSplits(cache, matrixMeta, rowIndex);

      case T_DOUBLE_SPARSE:
        return combineServerSparseDoubleRowSplits(getAllRowSplitsFromCache(cache), matrixMeta, rowIndex);

      case T_DOUBLE_SPARSE_LONGKEY:
        return combineServerSparseDoubleLongKeyRowSplits(getAllRowSplitsFromCache(cache), matrixMeta, rowIndex);

      case T_FLOAT_SPARSE:
        return combineServerSparseFloatRowSplits(getAllRowSplitsFromCache(cache), matrixMeta, rowIndex);

      case T_INT_DENSE:
        return combineDenseIntRowSplits(cache, matrixMeta, rowIndex);

      case T_FLOAT_DENSE:
        return combineDenseFloatRowSplits(cache, matrixMeta, rowIndex);

      case T_INT_SPARSE:
        return combineServerSparseIntRowSplits(getAllRowSplitsFromCache(cache), matrixMeta, rowIndex);

      case T_DOUBLE_SPARSE_COMPONENT:
        return combineComponentServerSparseDoubleRowSplits(getAllRowSplitsFromCache(cache), matrixMeta, rowIndex);

      case T_FLOAT_SPARSE_COMPONENT:
        return combineComponentServerSparseFloatRowSplits(getAllRowSplitsFromCache(cache), matrixMeta, rowIndex);

      case T_INT_SPARSE_COMPONENT:
        return combineComponentServerSparseIntRowSplits(getAllRowSplitsFromCache(cache), matrixMeta, rowIndex);

      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
        return combineCompServerSparseDoubleLongKeyRowSplits(getAllRowSplitsFromCache(cache), matrixMeta, rowIndex);

      default:
        throw new UnsupportedOperationException("Unsupport operation: merge " + rowType + " vector splits");
    }
  }

  private static List<ServerRow> getAllRowSplitsFromCache(GetRowPipelineCache cache)
    throws InterruptedException {
    int splitNum = cache.getTotalRequestNum();
    List<ServerRow> splits = new ArrayList<ServerRow>(splitNum);
    ServerRow split = null;

    int index = 0;
    while ((split = cache.poll()) != null && index < splitNum) {
      splits.add(split);
      index++;
    }

    return splits;
  }


  private static TVector combineDenseIntRowSplits(GetRowPipelineCache pipelineCache,
      MatrixMeta matrixMeta, int rowIndex) throws InterruptedException {
    int colNum = (int)matrixMeta.getColNum();
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
    int colNum = (int)matrixMeta.getColNum();
    float[] dataArray = new float[colNum];

    int clock = Integer.MAX_VALUE;
    while (true) {
      ServerRow split = pipelineCache.poll();
      if (split == null) {
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
    int colNum = (int)matrixMeta.getColNum();
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
    RowType rowType = matrixMeta.getRowType();

    switch (rowType) {
      case T_DOUBLE_DENSE:
        return combineServerDenseDoubleRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_FLOAT_DENSE:
        return combineServerDenseFloatRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_DOUBLE_SPARSE:
        return combineServerSparseDoubleRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_DOUBLE_SPARSE_LONGKEY:
        return combineServerSparseDoubleLongKeyRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_FLOAT_SPARSE:
        return combineServerSparseFloatRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_INT_DENSE:
        return combineServerDenseIntRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_INT_SPARSE:
        return combineServerSparseIntRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_DOUBLE_SPARSE_COMPONENT:
        return combineComponentServerSparseDoubleRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_FLOAT_SPARSE_COMPONENT:
        return combineComponentServerSparseFloatRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_INT_SPARSE_COMPONENT:
        return combineComponentServerSparseIntRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
        return combineCompServerSparseDoubleLongKeyRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_INT_ARBITRARY:
        return combineServerArbitaryIntRowSplits(rowSplits, matrixMeta, rowIndex);

      default:
        throw new UnsupportedOperationException("Unsupport operation: merge " + rowType + " vector splits");
    }
  }

  private static TVector combineServerArbitaryIntRowSplits(List<ServerRow> rowSplits,
      MatrixMeta matrixMeta, int rowIndex) {
    // TODO Auto-generated method stub
    return null;
  }

  private static TVector combineServerSparseIntRowSplits(List<ServerRow> rowSplits,
      MatrixMeta matrixMeta, int rowIndex) {
    int colNum = (int)matrixMeta.getColNum();
    int splitNum = rowSplits.size();
    int totalElemNum = 0;
    int[] lens = new int[splitNum];

    Collections.sort(rowSplits, serverRowComp);

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
    int colNum = (int)matrixMeta.getColNum();
    int[] dataArray = new int[colNum];

    Collections.sort(rowSplits, serverRowComp);

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
    int colNum = (int)matrixMeta.getColNum();
    int splitNum = rowSplits.size();
    int totalElemNum = 0;
    int[] lens = new int[splitNum];

    Collections.sort(rowSplits, serverRowComp);

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

  private static TVector combineServerSparseDoubleLongKeyRowSplits(List<ServerRow> rowSplits,
    MatrixMeta matrixMeta, int rowIndex) {
    long colNum = matrixMeta.getColNum();
    int splitNum = rowSplits.size();
    int totalElemNum = 0;
    int[] lens = new int[splitNum];

    Collections.sort(rowSplits, serverRowComp);

    int elemNum = 0;
    for (int i = 0; i < splitNum; i++) {
      elemNum = rowSplits.get(i).size();
      totalElemNum += elemNum;
      lens[i] = elemNum;
    }

    long[] indexes = new long[totalElemNum];
    double[] values = new double[totalElemNum];

    int clock = Integer.MAX_VALUE;
    int startPos = 0;
    assert(rowSplits.size() > 0);
    double defaultValue = ((ServerSparseDoubleLongKeyRow) rowSplits.get(0)).getDefaultValue();
    for (int i = 0; i < splitNum; i++) {
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
      ServerSparseDoubleLongKeyRow row = (ServerSparseDoubleLongKeyRow) rowSplits.get(i);
      row.mergeTo(indexes, values, startPos, lens[i]);
      assert(defaultValue == row.getDefaultValue());
      startPos += lens[i];
    }

    Long2DoubleOpenHashMap data = new Long2DoubleOpenHashMap(indexes, values);
    data.defaultReturnValue(defaultValue);
    SparseLongKeyDoubleVector row = new SparseLongKeyDoubleVector(colNum, data);
    row.setModelNnz(matrixMeta.getValidIndexNum());
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);
    row.setClock(clock);
    return row;
  }

  private static TVector combineCompServerSparseDoubleLongKeyRowSplits(List<ServerRow> rowSplits,
    MatrixMeta matrixMeta, int rowIndex) {
    List<PartitionKey> partitionKeys = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixMeta.getId(), rowIndex);
    assert rowSplits.size() == partitionKeys.size();
    Collections.sort(rowSplits, serverRowComp);
    Collections.sort(partitionKeys, partKeyComp);
    SparseLongKeyDoubleVector[] splits = new SparseLongKeyDoubleVector[rowSplits.size()];

    int size = rowSplits.size();
    int clock = Integer.MAX_VALUE;
    for(int i = 0; i < size; i++) {
      splits[i] = new SparseLongKeyDoubleVector(matrixMeta.getColNum(), ((ServerSparseDoubleLongKeyRow)(rowSplits.get(i))).getIndex2ValueMap());
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
    }
    CompSparseLongKeyDoubleVector row = new CompSparseLongKeyDoubleVector(matrixMeta.getId(),
      rowIndex, matrixMeta.getColNum(), partitionKeys.toArray(new PartitionKey[0]), splits);
    row.setClock(clock);
    return row;
  }

  private static TVector combineComponentServerSparseIntRowSplits(List<ServerRow> rowSplits,
    MatrixMeta matrixMeta, int rowIndex) {
    List<PartitionKey> partitionKeys = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixMeta.getId(), rowIndex);
    assert rowSplits.size() == partitionKeys.size();
    Collections.sort(rowSplits, serverRowComp);
    Collections.sort(partitionKeys, partKeyComp);
    TIntVector [] splits = new TIntVector[rowSplits.size()];

    int size = rowSplits.size();
    int clock = Integer.MAX_VALUE;
    for(int i = 0; i < size; i++) {
      splits[i] = new SparseIntVector((int)matrixMeta.getColNum(), ((ServerSparseIntRow)(rowSplits.get(i))).getData());
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
    }
    CompSparseIntVector row = new CompSparseIntVector(matrixMeta.getId(),
      rowIndex, (int)matrixMeta.getColNum(), partitionKeys.toArray(new PartitionKey[0]), splits);
    row.setClock(clock);
    return row;
  }

  private static TVector combineComponentServerSparseDoubleRowSplits(List<ServerRow> rowSplits,
    MatrixMeta matrixMeta, int rowIndex) {
    List<PartitionKey> partitionKeys = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixMeta.getId(), rowIndex);
    assert rowSplits.size() == partitionKeys.size();
    Collections.sort(rowSplits, serverRowComp);
    Collections.sort(partitionKeys, partKeyComp);
    TIntDoubleVector[] splits = new TIntDoubleVector[rowSplits.size()];

    int size = rowSplits.size();
    int clock = Integer.MAX_VALUE;
    for(int i = 0; i < size; i++) {
      splits[i] = new SparseDoubleVector((int)matrixMeta.getColNum(), ((ServerSparseDoubleRow)(rowSplits.get(i))).getData());
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
    }
    CompSparseDoubleVector row = new CompSparseDoubleVector(matrixMeta.getId(),
      rowIndex, (int)matrixMeta.getColNum(), partitionKeys.toArray(new PartitionKey[0]), splits);
    row.setClock(clock);
    return row;
  }

  private static TVector combineComponentServerSparseFloatRowSplits(List<ServerRow> rowSplits,
    MatrixMeta matrixMeta, int rowIndex) {
    long startTs = System.currentTimeMillis();
    List<PartitionKey> partitionKeys = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixMeta.getId(), rowIndex);
    assert rowSplits.size() == partitionKeys.size();
    Collections.sort(rowSplits, serverRowComp);
    Collections.sort(partitionKeys, partKeyComp);
    TFloatVector [] splits = new TFloatVector[rowSplits.size()];

    int size = rowSplits.size();
    int clock = Integer.MAX_VALUE;
    for(int i = 0; i < size; i++) {
      splits[i] = new SparseFloatVector((int)matrixMeta.getColNum(), ((ServerSparseFloatRow)(rowSplits.get(i))).getData());
      LOG.info("SparseFloatVector " + i + " sum=" + splits[i].sum());
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
    }
    CompSparseFloatVector row = new CompSparseFloatVector(matrixMeta.getId(),
      rowIndex, (int)matrixMeta.getColNum(), partitionKeys.toArray(new PartitionKey[0]), splits);
    row.setClock(clock);
    LOG.info("generate vector, split number=" + size + ", use time=" + (System.currentTimeMillis() - startTs));
    return row;
  }

  private static TVector combineServerSparseFloatRowSplits(List<ServerRow> rowSplits,
      MatrixMeta matrixMeta, int rowIndex) {
    int colNum = (int)matrixMeta.getColNum();
    int splitNum = rowSplits.size();
    int totalElemNum = 0;
    int[] lens = new int[splitNum];

    Collections.sort(rowSplits, serverRowComp);

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
    int colNum = (int)matrixMeta.getColNum();
    double[] dataArray = new double[colNum];

    Collections.sort(rowSplits, serverRowComp);

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
    int colNum = (int)matrixMeta.getColNum();
    float[] dataArray = new float[colNum];

    Collections.sort(rowSplits, serverRowComp);

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
