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


package com.tencent.angel.psagent.matrix.transport.adapter;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.vector.*;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.vector.*;
import com.tencent.angel.psagent.PSAgentContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * Row splits combine tool.
 */
public class RowSplitCombineUtils {
  private static final Log LOG = LogFactory.getLog(RowSplitCombineUtils.class);
  private static final Comparator serverRowComp = new StartColComparator();
  private static final Comparator partKeyComp = new PartitionKeyComparator();
  private static final float storageConvFactor = 0.25f;


  static class StartColComparator implements Comparator<ServerRow> {

    @Override public int compare(ServerRow r1, ServerRow r2) {
      return compareStartCol(r1, r2);
    }

    private int compareStartCol(ServerRow r1, ServerRow r2) {
      if (r1.getStartCol() > r2.getStartCol()) {
        return 1;
      } else if (r1.getStartCol() < r2.getStartCol()) {
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
      if (p1.getStartCol() > p2.getStartCol()) {
        return 1;
      } else if (p1.getStartCol() < p2.getStartCol()) {
        return -1;
      } else {
        return 0;
      }
    }
  }

  /**
   * Combine row splits of a single matrix row.
   *
   * @param rowSplits row splits
   * @param matrixId  matrix id
   * @param rowIndex  row index
   * @return TVector merged row
   */
  public static Vector combineServerRowSplits(List<ServerRow> rowSplits, int matrixId,
    int rowIndex) {
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    RowType rowType = matrixMeta.getRowType();

    switch (rowType) {
      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE:
        return combineServerIntDoubleRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_DOUBLE_DENSE_COMPONENT:
      case T_DOUBLE_SPARSE_COMPONENT:
        return combineCompServerIntDoubleRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE:
        return combineServerIntFloatRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_FLOAT_DENSE_COMPONENT:
      case T_FLOAT_SPARSE_COMPONENT:
        return combineCompServerIntFloatRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_INT_DENSE:
      case T_INT_SPARSE:
        return combineServerIntIntRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_INT_DENSE_COMPONENT:
      case T_INT_SPARSE_COMPONENT:
        return combineCompServerIntIntRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_LONG_DENSE:
      case T_LONG_SPARSE:
        return combineServerIntLongRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_LONG_DENSE_COMPONENT:
      case T_LONG_SPARSE_COMPONENT:
        return combineCompServerIntLongRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_DOUBLE_SPARSE_LONGKEY:
        return combineServerLongDoubleRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
        return combineCompServerLongDoubleRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_FLOAT_SPARSE_LONGKEY:
        return combineServerLongFloatRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_FLOAT_SPARSE_LONGKEY_COMPONENT:
        return combineCompServerLongFloatRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_INT_SPARSE_LONGKEY:
        return combineServerLongIntRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_INT_SPARSE_LONGKEY_COMPONENT:
        return combineCompServerLongIntRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_LONG_SPARSE_LONGKEY:
        return combineServerLongLongRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_LONG_SPARSE_LONGKEY_COMPONENT:
        return combineCompServerLongLongRowSplits(rowSplits, matrixMeta, rowIndex);

      default:
        throw new UnsupportedOperationException(
          "Unsupport operation: merge " + rowType + " vector splits");
    }
  }

  /**
   * Combine row splits to multi rows
   *
   * @param request index get rows request
   * @param cache   the result cache for sub-requests
   * @return Vector merged row
   * @throws InterruptedException interrupted while waiting for row splits
   */
  public static Vector[] combineIndexRowsSplits(IndexGetRowsRequest request,
    IndexGetRowsCache cache) throws Exception {
    MatrixMeta matrixMeta =
      PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(request.getMatrixId());
    RowType rowType = matrixMeta.getRowType();

    switch (rowType) {
      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE:
        return combineIntDoubleIndexRowsSplits(request, cache, matrixMeta);

      case T_DOUBLE_DENSE_COMPONENT:
      case T_DOUBLE_SPARSE_COMPONENT:
        return combineCompIntDoubleIndexRowsSplits(request, cache, matrixMeta);

      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE:
        return combineIntFloatIndexRowsSplits(request, cache, matrixMeta);

      case T_FLOAT_DENSE_COMPONENT:
      case T_FLOAT_SPARSE_COMPONENT:
        return combineCompIntFloatIndexRowsSplits(request, cache, matrixMeta);

      case T_INT_DENSE:
      case T_INT_SPARSE:
        return combineIntIntIndexRowsSplits(request, cache, matrixMeta);

      case T_INT_DENSE_COMPONENT:
      case T_INT_SPARSE_COMPONENT:
        return combineCompIntIntIndexRowsSplits(request, cache, matrixMeta);

      case T_LONG_DENSE:
      case T_LONG_SPARSE:
        return combineIntLongIndexRowsSplits(request, cache, matrixMeta);

      case T_LONG_DENSE_COMPONENT:
      case T_LONG_SPARSE_COMPONENT:
        return combineCompIntLongIndexRowsSplits(request, cache, matrixMeta);

      case T_DOUBLE_SPARSE_LONGKEY:
        return combineLongDoubleIndexRowsSplits(request, cache, matrixMeta);

      case T_DOUBLE_DENSE_LONGKEY_COMPONENT:
      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
        return combineCompLongDoubleIndexRowsSplits(request, cache, matrixMeta);

      case T_FLOAT_SPARSE_LONGKEY:
        return combineLongFloatIndexRowsSplits(request, cache, matrixMeta);

      case T_FLOAT_DENSE_LONGKEY_COMPONENT:
      case T_FLOAT_SPARSE_LONGKEY_COMPONENT:
        return combineCompLongFloatIndexRowsSplits(request, cache, matrixMeta);

      case T_INT_SPARSE_LONGKEY:
        return combineLongIntIndexRowsSplits(request, cache, matrixMeta);

      case T_INT_DENSE_LONGKEY_COMPONENT:
      case T_INT_SPARSE_LONGKEY_COMPONENT:
        return combineCompLongIntIndexRowsSplits(request, cache, matrixMeta);

      case T_LONG_SPARSE_LONGKEY:
        return combineLongLongIndexRowsSplits(request, cache, matrixMeta);

      case T_LONG_DENSE_LONGKEY_COMPONENT:
      case T_LONG_SPARSE_LONGKEY_COMPONENT:
        return combineCompLongLongIndexRowsSplits(request, cache, matrixMeta);

      default:
        throw new UnsupportedOperationException("unsupport row type " + rowType);
    }
  }

  private static Map<PartitionKey, IndexPartGetRowsResult> getPartToResultMap(
    IndexGetRowsCache cache) {
    List<IndexPartGetRowsResult> partResults = cache.getSubResponses();
    int size = partResults.size();
    Map<PartitionKey, IndexPartGetRowsResult> partKeyToResultMap =
      new HashMap<>(partResults.size());
    for (int i = 0; i < size; i++) {
      partKeyToResultMap.put(partResults.get(i).getPartKey(), partResults.get(i));
    }
    return partKeyToResultMap;
  }

  private static Vector[] combineIntDoubleIndexRowsSplits(IndexGetRowsRequest request,
    IndexGetRowsCache cache, MatrixMeta matrixMeta) {
    int[] rowIds = request.getRowIds();
    int[] colIds = ((IntIndexGetRowsRequest) request).getColIds();
    Map<PartitionKey, IndexPartGetRowsResult> partKeyToResultMap = getPartToResultMap(cache);

    Vector[] vectors = new Vector[rowIds.length];
    for (int i = 0; i < rowIds.length; i++) {
      IntDoubleVector vector =
        VFactory.sparseDoubleVector((int) matrixMeta.getColNum(), colIds.length);
      List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(request.getMatrixId(), rowIds[i]);
      for (PartitionKey partKey : parts) {
        IndexPartGetRowsResult result = partKeyToResultMap.get(partKey);
        if (result == null) {
          continue;
        }
        IndicesView colIdView = result.getColIds();
        double[] values = ((IndexPartGetRowsDoubleResult) result).getValues().get(rowIds[i]);
        for (int j = colIdView.startPos; j < colIdView.endPos; j++) {
          vector.set(colIds[j], values[j - colIdView.startPos]);
        }
      }

      vectors[i] = vector;
      vectors[i].setMatrixId(request.getMatrixId());
      vectors[i].setRowId(rowIds[i]);
    }

    return vectors;
  }

  private static Vector[] combineCompIntDoubleIndexRowsSplits(IndexGetRowsRequest request,
    IndexGetRowsCache cache, MatrixMeta matrixMeta) {
    int[] rowIds = request.getRowIds();
    int[] colIds = ((IntIndexGetRowsRequest) request).getColIds();
    Map<PartitionKey, IndexPartGetRowsResult> partKeyToResultMap = getPartToResultMap(cache);
    int subDim = (int) matrixMeta.getBlockColNum();

    Vector[] vectors = new Vector[rowIds.length];
    for (int i = 0; i < rowIds.length; i++) {
      List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(request.getMatrixId(), rowIds[i]);
      IntDoubleVector[] subVecs = new IntDoubleVector[parts.size()];
      int index = 0;
      for (PartitionKey partKey : parts) {
        IndexPartGetRowsResult result = partKeyToResultMap.get(partKey);
        if (result == null) {
          subVecs[index++] = VFactory.sparseDoubleVector((int) matrixMeta.getColNum(), 0);
          continue;
        }

        IndicesView colIdView = result.getColIds();
        IntDoubleVector vector = VFactory
          .sparseDoubleVector((int) matrixMeta.getColNum(), colIdView.endPos - colIdView.startPos);
        double[] values = ((IndexPartGetRowsDoubleResult) result).getValues().get(rowIds[i]);
        for (int j = colIdView.startPos; j < colIdView.endPos; j++) {
          vector.set(colIds[j] - (int) partKey.getStartCol(), values[j - colIdView.startPos]);
        }

        subVecs[index++] = vector;
      }

      vectors[i] = VFactory.compIntDoubleVector((int) matrixMeta.getColNum(), subVecs, subDim);
      vectors[i].setMatrixId(request.getMatrixId());
      vectors[i].setRowId(rowIds[i]);
    }

    return vectors;
  }


  private static Vector[] combineIntFloatIndexRowsSplits(IndexGetRowsRequest request,
    IndexGetRowsCache cache, MatrixMeta matrixMeta) {
    int[] rowIds = request.getRowIds();
    int[] colIds = ((IntIndexGetRowsRequest) request).getColIds();
    Map<PartitionKey, IndexPartGetRowsResult> partKeyToResultMap = getPartToResultMap(cache);

    Vector[] vectors = new Vector[rowIds.length];
    for (int i = 0; i < rowIds.length; i++) {
      IntFloatVector vector =
        VFactory.sparseFloatVector((int) matrixMeta.getColNum(), colIds.length);
      List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(request.getMatrixId(), rowIds[i]);
      for (PartitionKey partKey : parts) {
        IndexPartGetRowsResult result = partKeyToResultMap.get(partKey);
        if (result == null) {
          continue;
        }
        IndicesView colIdView = result.getColIds();
        float[] values = ((IndexPartGetRowsFloatResult) result).getValues().get(rowIds[i]);
        for (int j = colIdView.startPos; j < colIdView.endPos; j++) {
          vector.set(colIds[j], values[j - colIdView.startPos]);
        }
      }
      vectors[i] = vector;
      vectors[i].setMatrixId(request.getMatrixId());
      vectors[i].setRowId(rowIds[i]);
    }

    return vectors;
  }

  private static Vector[] combineCompIntFloatIndexRowsSplits(IndexGetRowsRequest request,
    IndexGetRowsCache cache, MatrixMeta matrixMeta) {
    int[] rowIds = request.getRowIds();
    int[] colIds = ((IntIndexGetRowsRequest) request).getColIds();
    Map<PartitionKey, IndexPartGetRowsResult> partKeyToResultMap = getPartToResultMap(cache);
    int subDim = (int) matrixMeta.getBlockColNum();

    Vector[] vectors = new Vector[rowIds.length];
    for (int i = 0; i < rowIds.length; i++) {
      List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(request.getMatrixId(), rowIds[i]);
      IntFloatVector[] subVecs = new IntFloatVector[parts.size()];
      int index = 0;
      for (PartitionKey partKey : parts) {
        IndexPartGetRowsResult result = partKeyToResultMap.get(partKey);
        if (result == null) {
          subVecs[index++] = VFactory.sparseFloatVector((int) matrixMeta.getColNum(), 0);
          continue;
        }

        IndicesView colIdView = result.getColIds();
        IntFloatVector vector = VFactory
          .sparseFloatVector((int) matrixMeta.getColNum(), colIdView.endPos - colIdView.startPos);
        float[] values = ((IndexPartGetRowsFloatResult) result).getValues().get(rowIds[i]);
        for (int j = colIdView.startPos; j < colIdView.endPos; j++) {
          vector.set(colIds[j] - (int) partKey.getStartCol(), values[j - colIdView.startPos]);
        }
        subVecs[index++] = vector;
      }

      vectors[i] = VFactory.compIntFloatVector((int) matrixMeta.getColNum(), subVecs, subDim);
      vectors[i].setMatrixId(request.getMatrixId());
      vectors[i].setRowId(rowIds[i]);
    }

    return vectors;
  }


  private static Vector[] combineIntIntIndexRowsSplits(IndexGetRowsRequest request,
    IndexGetRowsCache cache, MatrixMeta matrixMeta) {
    int[] rowIds = request.getRowIds();
    int[] colIds = ((IntIndexGetRowsRequest) request).getColIds();
    Map<PartitionKey, IndexPartGetRowsResult> partKeyToResultMap = getPartToResultMap(cache);

    Vector[] vectors = new Vector[rowIds.length];
    for (int i = 0; i < rowIds.length; i++) {
      IntIntVector vector = VFactory.sparseIntVector((int) matrixMeta.getColNum(), colIds.length);
      List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(request.getMatrixId(), rowIds[i]);
      for (PartitionKey partKey : parts) {
        IndexPartGetRowsResult result = partKeyToResultMap.get(partKey);
        if (result == null) {
          continue;
        }
        IndicesView colIdView = result.getColIds();
        int[] values = ((IndexPartGetRowsIntResult) result).getValues().get(rowIds[i]);
        for (int j = colIdView.startPos; j < colIdView.endPos; j++) {
          vector.set(colIds[j], values[j - colIdView.startPos]);
        }
      }
      vectors[i] = vector;
      vectors[i].setMatrixId(request.getMatrixId());
      vectors[i].setRowId(rowIds[i]);
    }

    return vectors;
  }

  private static Vector[] combineCompIntIntIndexRowsSplits(IndexGetRowsRequest request,
    IndexGetRowsCache cache, MatrixMeta matrixMeta) {
    int[] rowIds = request.getRowIds();
    int[] colIds = ((IntIndexGetRowsRequest) request).getColIds();
    Map<PartitionKey, IndexPartGetRowsResult> partKeyToResultMap = getPartToResultMap(cache);
    int subDim = (int) matrixMeta.getBlockColNum();

    Vector[] vectors = new Vector[rowIds.length];
    for (int i = 0; i < rowIds.length; i++) {
      List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(request.getMatrixId(), rowIds[i]);
      IntIntVector[] subVecs = new IntIntVector[parts.size()];
      int index = 0;
      for (PartitionKey partKey : parts) {
        IndexPartGetRowsResult result = partKeyToResultMap.get(partKey);
        if (result == null) {
          subVecs[index++] = VFactory.sparseIntVector((int) matrixMeta.getColNum(), 0);
          continue;
        }

        IndicesView colIdView = result.getColIds();
        IntIntVector vector = VFactory
          .sparseIntVector((int) matrixMeta.getColNum(), colIdView.endPos - colIdView.startPos);
        int[] values = ((IndexPartGetRowsIntResult) result).getValues().get(rowIds[i]);
        for (int j = colIdView.startPos; j < colIdView.endPos; j++) {
          vector.set(colIds[j] - (int) partKey.getStartCol(), values[j - colIdView.startPos]);
        }
        subVecs[index++] = vector;
      }

      vectors[i] = VFactory.compIntIntVector((int) matrixMeta.getColNum(), subVecs, subDim);
      vectors[i].setMatrixId(request.getMatrixId());
      vectors[i].setRowId(rowIds[i]);
    }

    return vectors;
  }

  private static Vector[] combineIntLongIndexRowsSplits(IndexGetRowsRequest request,
    IndexGetRowsCache cache, MatrixMeta matrixMeta) {
    int[] rowIds = request.getRowIds();
    int[] colIds = ((IntIndexGetRowsRequest) request).getColIds();
    Map<PartitionKey, IndexPartGetRowsResult> partKeyToResultMap = getPartToResultMap(cache);

    Vector[] vectors = new Vector[rowIds.length];
    for (int i = 0; i < rowIds.length; i++) {
      IntLongVector vector = VFactory.sparseLongVector((int) matrixMeta.getColNum(), colIds.length);
      List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(request.getMatrixId(), rowIds[i]);
      for (PartitionKey partKey : parts) {
        IndexPartGetRowsResult result = partKeyToResultMap.get(partKey);
        if (result == null) {
          continue;
        }
        IndicesView colIdView = result.getColIds();
        long[] values = ((IndexPartGetRowsLongResult) result).getValues().get(rowIds[i]);
        for (int j = colIdView.startPos; j < colIdView.endPos; j++) {
          vector.set(colIds[j], values[j - colIdView.startPos]);
        }
      }
      vectors[i] = vector;
      vectors[i].setMatrixId(request.getMatrixId());
      vectors[i].setRowId(rowIds[i]);
    }

    return vectors;
  }

  private static Vector[] combineCompIntLongIndexRowsSplits(IndexGetRowsRequest request,
    IndexGetRowsCache cache, MatrixMeta matrixMeta) {
    int[] rowIds = request.getRowIds();
    int[] colIds = ((IntIndexGetRowsRequest) request).getColIds();
    Map<PartitionKey, IndexPartGetRowsResult> partKeyToResultMap = getPartToResultMap(cache);
    int subDim = (int) matrixMeta.getBlockColNum();

    Vector[] vectors = new Vector[rowIds.length];
    for (int i = 0; i < rowIds.length; i++) {
      List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(request.getMatrixId(), rowIds[i]);
      IntLongVector[] subVecs = new IntLongVector[parts.size()];
      int index = 0;
      for (PartitionKey partKey : parts) {
        IndexPartGetRowsResult result = partKeyToResultMap.get(partKey);
        if (result == null) {
          subVecs[index++] = VFactory.sparseLongVector((int) matrixMeta.getColNum(), 0);
          continue;
        }

        IndicesView colIdView = result.getColIds();
        IntLongVector vector = VFactory
          .sparseLongVector((int) matrixMeta.getColNum(), colIdView.endPos - colIdView.startPos);
        long[] values = ((IndexPartGetRowsLongResult) result).getValues().get(rowIds[i]);
        for (int j = colIdView.startPos; j < colIdView.endPos; j++) {
          vector.set(colIds[j] - (int) partKey.getStartCol(), values[j - colIdView.startPos]);
        }
        subVecs[index++] = vector;
      }

      vectors[i] = VFactory.compIntLongVector((int) matrixMeta.getColNum(), subVecs, subDim);
      vectors[i].setMatrixId(request.getMatrixId());
      vectors[i].setRowId(rowIds[i]);
    }

    return vectors;
  }


  private static Vector[] combineLongDoubleIndexRowsSplits(IndexGetRowsRequest request,
    IndexGetRowsCache cache, MatrixMeta matrixMeta) {
    int[] rowIds = request.getRowIds();
    long[] colIds = ((LongIndexGetRowsRequest) request).getColIds();
    Map<PartitionKey, IndexPartGetRowsResult> partKeyToResultMap = getPartToResultMap(cache);

    Vector[] vectors = new Vector[rowIds.length];
    for (int i = 0; i < rowIds.length; i++) {
      LongDoubleVector vector =
        VFactory.sparseLongKeyDoubleVector(matrixMeta.getColNum(), colIds.length);
      List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(request.getMatrixId(), rowIds[i]);
      for (PartitionKey partKey : parts) {
        IndexPartGetRowsResult result = partKeyToResultMap.get(partKey);
        if (result == null) {
          continue;
        }
        IndicesView colIdView = result.getColIds();
        double[] values = ((IndexPartGetRowsDoubleResult) result).getValues().get(rowIds[i]);
        for (int j = colIdView.startPos; j < colIdView.endPos; j++) {
          vector.set(colIds[j], values[j - colIdView.startPos]);
        }
      }
      vectors[i] = vector;
      vectors[i].setMatrixId(request.getMatrixId());
      vectors[i].setRowId(rowIds[i]);
    }

    return vectors;
  }

  private static Vector[] combineCompLongDoubleIndexRowsSplits(IndexGetRowsRequest request,
    IndexGetRowsCache cache, MatrixMeta matrixMeta) {
    int[] rowIds = request.getRowIds();
    long[] colIds = ((LongIndexGetRowsRequest) request).getColIds();
    Map<PartitionKey, IndexPartGetRowsResult> partKeyToResultMap = getPartToResultMap(cache);
    long subDim = matrixMeta.getBlockColNum();

    Vector[] vectors = new Vector[rowIds.length];
    for (int i = 0; i < rowIds.length; i++) {
      List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(request.getMatrixId(), rowIds[i]);
      LongDoubleVector[] subVecs = new LongDoubleVector[parts.size()];
      int index = 0;
      for (PartitionKey partKey : parts) {
        IndexPartGetRowsResult result = partKeyToResultMap.get(partKey);
        if (result == null) {
          subVecs[index++] = VFactory.sparseLongKeyDoubleVector((int) matrixMeta.getColNum(), 0);
          continue;
        }

        IndicesView colIdView = result.getColIds();
        LongDoubleVector vector = VFactory
          .sparseLongKeyDoubleVector(matrixMeta.getColNum(), colIdView.endPos - colIdView.startPos);
        double[] values = ((IndexPartGetRowsDoubleResult) result).getValues().get(rowIds[i]);
        for (int j = colIdView.startPos; j < colIdView.endPos; j++) {
          vector.set(colIds[j] - (int) partKey.getStartCol(), values[j - colIdView.startPos]);
        }
        subVecs[index++] = vector;
      }

      vectors[i] = VFactory.compLongDoubleVector(matrixMeta.getColNum(), subVecs, subDim);
      vectors[i].setMatrixId(request.getMatrixId());
      vectors[i].setRowId(rowIds[i]);
    }

    return vectors;
  }


  private static Vector[] combineLongFloatIndexRowsSplits(IndexGetRowsRequest request,
    IndexGetRowsCache cache, MatrixMeta matrixMeta) {
    int[] rowIds = request.getRowIds();
    long[] colIds = ((LongIndexGetRowsRequest) request).getColIds();
    Map<PartitionKey, IndexPartGetRowsResult> partKeyToResultMap = getPartToResultMap(cache);

    Vector[] vectors = new Vector[rowIds.length];
    for (int i = 0; i < rowIds.length; i++) {
      LongFloatVector vector =
        VFactory.sparseLongKeyFloatVector(matrixMeta.getColNum(), colIds.length);
      List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(request.getMatrixId(), rowIds[i]);
      for (PartitionKey partKey : parts) {
        IndexPartGetRowsResult result = partKeyToResultMap.get(partKey);
        if (result == null) {
          continue;
        }
        IndicesView colIdView = result.getColIds();
        float[] values = ((IndexPartGetRowsFloatResult) result).getValues().get(rowIds[i]);
        for (int j = colIdView.startPos; j < colIdView.endPos; j++) {
          vector.set(colIds[j], values[j - colIdView.startPos]);
        }
      }
      vectors[i] = vector;
      vectors[i].setMatrixId(request.getMatrixId());
      vectors[i].setRowId(rowIds[i]);
    }

    return vectors;
  }

  private static Vector[] combineCompLongFloatIndexRowsSplits(IndexGetRowsRequest request,
    IndexGetRowsCache cache, MatrixMeta matrixMeta) {
    int[] rowIds = request.getRowIds();
    long[] colIds = ((LongIndexGetRowsRequest) request).getColIds();
    Map<PartitionKey, IndexPartGetRowsResult> partKeyToResultMap = getPartToResultMap(cache);
    long subDim = matrixMeta.getBlockColNum();

    Vector[] vectors = new Vector[rowIds.length];
    for (int i = 0; i < rowIds.length; i++) {
      List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(request.getMatrixId(), rowIds[i]);
      LongFloatVector[] subVecs = new LongFloatVector[parts.size()];
      int index = 0;
      for (PartitionKey partKey : parts) {
        IndexPartGetRowsResult result = partKeyToResultMap.get(partKey);
        if (result == null) {
          subVecs[index++] = VFactory.sparseLongKeyFloatVector((int) matrixMeta.getColNum(), 0);
          continue;
        }

        IndicesView colIdView = result.getColIds();
        LongFloatVector vector = VFactory
          .sparseLongKeyFloatVector(matrixMeta.getColNum(), colIdView.endPos - colIdView.startPos);
        float[] values = ((IndexPartGetRowsFloatResult) result).getValues().get(rowIds[i]);
        for (int j = colIdView.startPos; j < colIdView.endPos; j++) {
          vector.set(colIds[j] - (int) partKey.getStartCol(), values[j - colIdView.startPos]);
        }
        subVecs[index++] = vector;
      }

      vectors[i] = VFactory.compLongFloatVector(matrixMeta.getColNum(), subVecs, subDim);
      vectors[i].setMatrixId(request.getMatrixId());
      vectors[i].setRowId(rowIds[i]);
    }

    return vectors;
  }


  private static Vector[] combineLongIntIndexRowsSplits(IndexGetRowsRequest request,
    IndexGetRowsCache cache, MatrixMeta matrixMeta) {
    int[] rowIds = request.getRowIds();
    long[] colIds = ((LongIndexGetRowsRequest) request).getColIds();
    Map<PartitionKey, IndexPartGetRowsResult> partKeyToResultMap = getPartToResultMap(cache);

    Vector[] vectors = new Vector[rowIds.length];
    for (int i = 0; i < rowIds.length; i++) {
      LongIntVector vector = VFactory.sparseLongKeyIntVector(matrixMeta.getColNum(), colIds.length);
      List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(request.getMatrixId(), rowIds[i]);
      for (PartitionKey partKey : parts) {
        IndexPartGetRowsResult result = partKeyToResultMap.get(partKey);
        if (result == null) {
          continue;
        }
        IndicesView colIdView = result.getColIds();
        int[] values = ((IndexPartGetRowsIntResult) result).getValues().get(rowIds[i]);
        for (int j = colIdView.startPos; j < colIdView.endPos; j++) {
          vector.set(colIds[j], values[j - colIdView.startPos]);
        }
      }
      vectors[i] = vector;
      vectors[i].setMatrixId(request.getMatrixId());
      vectors[i].setRowId(rowIds[i]);
    }

    return vectors;
  }

  private static Vector[] combineCompLongIntIndexRowsSplits(IndexGetRowsRequest request,
    IndexGetRowsCache cache, MatrixMeta matrixMeta) {
    int[] rowIds = request.getRowIds();
    long[] colIds = ((LongIndexGetRowsRequest) request).getColIds();
    Map<PartitionKey, IndexPartGetRowsResult> partKeyToResultMap = getPartToResultMap(cache);
    long subDim = matrixMeta.getBlockColNum();

    Vector[] vectors = new Vector[rowIds.length];
    for (int i = 0; i < rowIds.length; i++) {
      List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(request.getMatrixId(), rowIds[i]);
      LongIntVector[] subVecs = new LongIntVector[parts.size()];
      int index = 0;
      for (PartitionKey partKey : parts) {
        IndexPartGetRowsResult result = partKeyToResultMap.get(partKey);
        if (result == null) {
          subVecs[index++] = VFactory.sparseLongKeyIntVector((int) matrixMeta.getColNum(), 0);
          continue;
        }

        IndicesView colIdView = result.getColIds();
        LongIntVector vector = VFactory
          .sparseLongKeyIntVector(matrixMeta.getColNum(), colIdView.endPos - colIdView.startPos);
        int[] values = ((IndexPartGetRowsIntResult) result).getValues().get(rowIds[i]);
        for (int j = colIdView.startPos; j < colIdView.endPos; j++) {
          vector.set(colIds[j] - (int) partKey.getStartCol(), values[j - colIdView.startPos]);
        }
        subVecs[index++] = vector;
      }

      vectors[i] = VFactory.compLongIntVector(matrixMeta.getColNum(), subVecs, subDim);
      vectors[i].setMatrixId(request.getMatrixId());
      vectors[i].setRowId(rowIds[i]);

    }

    return vectors;
  }



  private static Vector[] combineLongLongIndexRowsSplits(IndexGetRowsRequest request,
    IndexGetRowsCache cache, MatrixMeta matrixMeta) {
    int[] rowIds = request.getRowIds();
    long[] colIds = ((LongIndexGetRowsRequest) request).getColIds();
    Map<PartitionKey, IndexPartGetRowsResult> partKeyToResultMap = getPartToResultMap(cache);

    Vector[] vectors = new Vector[rowIds.length];
    for (int i = 0; i < rowIds.length; i++) {
      LongLongVector vector =
        VFactory.sparseLongKeyLongVector(matrixMeta.getColNum(), colIds.length);
      List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(request.getMatrixId(), rowIds[i]);
      for (PartitionKey partKey : parts) {
        IndexPartGetRowsResult result = partKeyToResultMap.get(partKey);
        if (result == null) {
          continue;
        }
        IndicesView colIdView = result.getColIds();
        long[] values = ((IndexPartGetRowsLongResult) result).getValues().get(rowIds[i]);
        for (int j = colIdView.startPos; j < colIdView.endPos; j++) {
          vector.set(colIds[j], values[j - colIdView.startPos]);
        }
      }
      vectors[i] = vector;
      vectors[i].setMatrixId(request.getMatrixId());
      vectors[i].setRowId(rowIds[i]);
    }

    return vectors;
  }

  private static Vector[] combineCompLongLongIndexRowsSplits(IndexGetRowsRequest request,
    IndexGetRowsCache cache, MatrixMeta matrixMeta) {
    int[] rowIds = request.getRowIds();
    long[] colIds = ((LongIndexGetRowsRequest) request).getColIds();
    Map<PartitionKey, IndexPartGetRowsResult> partKeyToResultMap = getPartToResultMap(cache);
    long subDim = matrixMeta.getBlockColNum();

    Vector[] vectors = new Vector[rowIds.length];
    for (int i = 0; i < rowIds.length; i++) {
      List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(request.getMatrixId(), rowIds[i]);
      LongLongVector[] subVecs = new LongLongVector[parts.size()];
      int index = 0;
      for (PartitionKey partKey : parts) {
        IndexPartGetRowsResult result = partKeyToResultMap.get(partKey);
        if (result == null) {
          subVecs[index++] = VFactory.sparseLongKeyLongVector((int) matrixMeta.getColNum(), 0);
          continue;
        }

        IndicesView colIdView = result.getColIds();
        LongLongVector vector = VFactory
          .sparseLongKeyLongVector(matrixMeta.getColNum(), colIdView.endPos - colIdView.startPos);
        long[] values = ((IndexPartGetRowsLongResult) result).getValues().get(rowIds[i]);
        for (int j = colIdView.startPos; j < colIdView.endPos; j++) {
          vector.set(colIds[j] - (int) partKey.getStartCol(), values[j - colIdView.startPos]);
        }
        subVecs[index++] = vector;
      }

      vectors[i] = VFactory.compLongLongVector(matrixMeta.getColNum(), subVecs, subDim);
      vectors[i].setMatrixId(request.getMatrixId());
      vectors[i].setRowId(rowIds[i]);
    }

    return vectors;
  }

  /**
   * Combine the row splits use pipeline mode.
   *
   * @param request index get request
   * @param cache   the result cache for GET_ROW sub-requests
   * @return Vector merged row
   * @throws InterruptedException interrupted while waiting for row splits
   */
  public static Vector combineIndexRowSplits(IndexGetRowRequest request, IndexGetRowCache cache)
    throws Exception {
    MatrixMeta matrixMeta =
      PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(request.getMatrixId());
    RowType rowType = matrixMeta.getRowType();

    switch (rowType) {
      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE:
        return combineIntDoubleIndexRowSplits(request, cache, matrixMeta);

      case T_DOUBLE_DENSE_COMPONENT:
      case T_DOUBLE_SPARSE_COMPONENT:
        return combineCompIntDoubleIndexRowSplits(request, cache, matrixMeta);

      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE:
        return combineIntFloatIndexRowSplits(request, cache, matrixMeta);

      case T_FLOAT_DENSE_COMPONENT:
      case T_FLOAT_SPARSE_COMPONENT:
        return combineCompIntFloatIndexRowSplits(request, cache, matrixMeta);

      case T_INT_DENSE:
      case T_INT_SPARSE:
        return combineIntIntIndexRowSplits(request, cache, matrixMeta);

      case T_INT_DENSE_COMPONENT:
      case T_INT_SPARSE_COMPONENT:
        return combineCompIntIntIndexRowSplits(request, cache, matrixMeta);

      case T_LONG_DENSE:
      case T_LONG_SPARSE:
        return combineIntLongIndexRowSplits(request, cache, matrixMeta);

      case T_LONG_DENSE_COMPONENT:
      case T_LONG_SPARSE_COMPONENT:
        return combineCompIntLongIndexRowSplits(request, cache, matrixMeta);

      case T_DOUBLE_SPARSE_LONGKEY:
        return combineLongDoubleIndexRowSplits(request, cache, matrixMeta);

      case T_DOUBLE_DENSE_LONGKEY_COMPONENT:
      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
        return combineCompLongDoubleIndexRowSplits(request, cache, matrixMeta);

      case T_FLOAT_SPARSE_LONGKEY:
        return combineLongFloatIndexRowSplits(request, cache, matrixMeta);

      case T_FLOAT_DENSE_LONGKEY_COMPONENT:
      case T_FLOAT_SPARSE_LONGKEY_COMPONENT:
        return combineCompLongFloatIndexRowSplits(request, cache, matrixMeta);

      case T_INT_SPARSE_LONGKEY:
        return combineLongIntIndexRowSplits(request, cache, matrixMeta);

      case T_INT_DENSE_LONGKEY_COMPONENT:
      case T_INT_SPARSE_LONGKEY_COMPONENT:
        return combineCompLongIntIndexRowSplits(request, cache, matrixMeta);

      case T_LONG_SPARSE_LONGKEY:
        return combineLongLongIndexRowSplits(request, cache, matrixMeta);

      case T_LONG_DENSE_LONGKEY_COMPONENT:
      case T_LONG_SPARSE_LONGKEY_COMPONENT:
        return combineCompLongLongIndexRowSplits(request, cache, matrixMeta);

      default:
        throw new UnsupportedOperationException("unsupport row type " + rowType);
    }
  }

  private static Vector combineIntDoubleIndexRowSplits(IndexGetRowRequest request,
    IndexGetRowCache cache, MatrixMeta matrixMeta) {
    List<IndexPartGetRowResult> partResults = cache.getSubResponses();
    IntDoubleVector vector = VFactory.sparseDoubleVector((int) matrixMeta.getColNum(),
      ((IntIndexGetRowRequest) request).getIndices().length);
    int[] indices = ((IntIndexGetRowRequest) request).getIndices();
    int size = partResults.size();
    for (int i = 0; i < size; i++) {
      IntIndicesView indicesView = (IntIndicesView) partResults.get(i).getIndices();
      double[] values = ((IndexPartGetRowDoubleResult) (partResults.get(i))).getValues();
      for (int j = indicesView.startPos; j < indicesView.endPos; j++) {
        vector.set(indices[j], values[j - indicesView.startPos]);
      }
    }

    return vector;
  }

  private static Vector combineCompIntDoubleIndexRowSplits(IndexGetRowRequest request,
    IndexGetRowCache cache, MatrixMeta matrixMeta) {
    List<IndexPartGetRowResult> partResults = cache.getSubResponses();
    int subDim = (int) matrixMeta.getBlockColNum();
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager()
      .getPartitions(request.getMatrixId(), request.getRowId());
    int partNum = parts.size();
    IntDoubleVector[] subVecs = new IntDoubleVector[partNum];

    int[] indices = ((IntIndexGetRowRequest) request).getIndices();
    int size = partResults.size();
    for (int i = 0; i < size; i++) {
      IndexPartGetRowDoubleResult subResult = (IndexPartGetRowDoubleResult) partResults.get(i);
      double[] values = subResult.getValues();
      IntIndicesView indicesView = (IntIndicesView) subResult.getIndices();
      PartitionKey partKey = subResult.getPartKey();
      IntDoubleVector subVec =
        VFactory.sparseDoubleVector(subDim, indicesView.endPos - indicesView.startPos);
      for (int j = indicesView.startPos; j < indicesView.endPos; j++) {
        subVec.set(indices[j] - (int) partKey.getStartCol(), values[j - indicesView.startPos]);
      }
      subVecs[partKey.getPartitionId()] = subVec;
    }

    for (int i = 0; i < partNum; i++) {
      if (subVecs[i] == null) {
        subVecs[i] = VFactory.sparseDoubleVector(subDim, 0);
      }
    }

    CompIntDoubleVector vector =
      VFactory.compIntDoubleVector((int) matrixMeta.getColNum(), subVecs, subDim);
    vector.setMatrixId(request.getMatrixId());
    vector.setRowId(request.getRowId());
    return vector;
  }


  private static Vector combineIntFloatIndexRowSplits(IndexGetRowRequest request,
    IndexGetRowCache cache, MatrixMeta matrixMeta) {
    List<IndexPartGetRowResult> partResults = cache.getSubResponses();
    IntFloatVector vector = VFactory.sparseFloatVector((int) matrixMeta.getColNum(),
      ((IntIndexGetRowRequest) request).getIndices().length);
    int[] indices = ((IntIndexGetRowRequest) request).getIndices();
    int size = partResults.size();
    for (int i = 0; i < size; i++) {
      IntIndicesView indicesView = (IntIndicesView) partResults.get(i).getIndices();
      float[] values = ((IndexPartGetRowFloatResult) (partResults.get(i))).getValues();
      for (int j = indicesView.startPos; j < indicesView.endPos; j++) {
        vector.set(indices[j], values[j - indicesView.startPos]);
      }
    }

    return vector;
  }

  private static Vector combineCompIntFloatIndexRowSplits(IndexGetRowRequest request,
    IndexGetRowCache cache, MatrixMeta matrixMeta) {
    List<IndexPartGetRowResult> partResults = cache.getSubResponses();
    int subDim = (int) matrixMeta.getBlockColNum();
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager()
      .getPartitions(request.getMatrixId(), request.getRowId());
    int partNum = parts.size();
    IntFloatVector[] subVecs = new IntFloatVector[partNum];

    int[] indices = ((IntIndexGetRowRequest) request).getIndices();
    int size = partResults.size();
    for (int i = 0; i < size; i++) {
      IndexPartGetRowFloatResult subResult = (IndexPartGetRowFloatResult) partResults.get(i);
      float[] values = subResult.getValues();
      IntIndicesView indicesView = (IntIndicesView) subResult.getIndices();
      PartitionKey partKey = subResult.getPartKey();
      IntFloatVector subVec =
        VFactory.sparseFloatVector(subDim, indicesView.endPos - indicesView.startPos);
      for (int j = indicesView.startPos; j < indicesView.endPos; j++) {
        subVec.set(indices[j] - (int) partKey.getStartCol(), values[j - indicesView.startPos]);
      }
      subVecs[partKey.getPartitionId()] = subVec;
    }

    for (int i = 0; i < partNum; i++) {
      if (subVecs[i] == null) {
        subVecs[i] = VFactory.sparseFloatVector(subDim, 0);
      }
    }

    CompIntFloatVector vector =
      VFactory.compIntFloatVector((int) matrixMeta.getColNum(), subVecs, subDim);
    vector.setMatrixId(request.getMatrixId());
    vector.setRowId(request.getRowId());
    return vector;
  }


  private static Vector combineIntIntIndexRowSplits(IndexGetRowRequest request,
    IndexGetRowCache cache, MatrixMeta matrixMeta) {
    List<IndexPartGetRowResult> partResults = cache.getSubResponses();
    IntIntVector vector = VFactory.sparseIntVector((int) matrixMeta.getColNum(),
      ((IntIndexGetRowRequest) request).getIndices().length);
    int[] indices = ((IntIndexGetRowRequest) request).getIndices();
    int size = partResults.size();
    for (int i = 0; i < size; i++) {
      IntIndicesView indicesView = (IntIndicesView) partResults.get(i).getIndices();
      int[] values = ((IndexPartGetRowIntResult) (partResults.get(i))).getValues();
      for (int j = indicesView.startPos; j < indicesView.endPos; j++) {
        vector.set(indices[j], values[j - indicesView.startPos]);
      }
    }

    return vector;
  }

  private static Vector combineCompIntIntIndexRowSplits(IndexGetRowRequest request,
    IndexGetRowCache cache, MatrixMeta matrixMeta) {
    List<IndexPartGetRowResult> partResults = cache.getSubResponses();
    int subDim = (int) matrixMeta.getBlockColNum();
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager()
      .getPartitions(request.getMatrixId(), request.getRowId());
    int partNum = parts.size();
    IntIntVector[] subVecs = new IntIntVector[partNum];

    int[] indices = ((IntIndexGetRowRequest) request).getIndices();
    int size = partResults.size();
    for (int i = 0; i < size; i++) {
      IndexPartGetRowIntResult subResult = (IndexPartGetRowIntResult) partResults.get(i);
      int[] values = subResult.getValues();
      IntIndicesView indicesView = (IntIndicesView) subResult.getIndices();
      PartitionKey partKey = subResult.getPartKey();
      IntIntVector subVec =
        VFactory.sparseIntVector(subDim, indicesView.endPos - indicesView.startPos);
      for (int j = indicesView.startPos; j < indicesView.endPos; j++) {
        subVec.set(indices[j] - (int) partKey.getStartCol(), values[j - indicesView.startPos]);
      }
      subVecs[partKey.getPartitionId()] = subVec;
    }

    for (int i = 0; i < partNum; i++) {
      if (subVecs[i] == null) {
        subVecs[i] = VFactory.sparseIntVector(subDim, 0);
      }
    }

    CompIntIntVector vector =
      VFactory.compIntIntVector((int) matrixMeta.getColNum(), subVecs, subDim);
    vector.setMatrixId(request.getMatrixId());
    vector.setRowId(request.getRowId());
    return vector;
  }

  private static Vector combineIntLongIndexRowSplits(IndexGetRowRequest request,
    IndexGetRowCache cache, MatrixMeta matrixMeta) {
    List<IndexPartGetRowResult> partResults = cache.getSubResponses();
    IntLongVector vector = VFactory.sparseLongVector((int) matrixMeta.getColNum(),
      ((IntIndexGetRowRequest) request).getIndices().length);
    int[] indices = ((IntIndexGetRowRequest) request).getIndices();
    int size = partResults.size();
    for (int i = 0; i < size; i++) {
      IntIndicesView indicesView = (IntIndicesView) partResults.get(i).getIndices();
      long[] values = ((IndexPartGetRowLongResult) (partResults.get(i))).getValues();
      for (int j = indicesView.startPos; j < indicesView.endPos; j++) {
        vector.set(indices[j], values[j - indicesView.startPos]);
      }
    }

    return vector;
  }

  private static Vector combineCompIntLongIndexRowSplits(IndexGetRowRequest request,
    IndexGetRowCache cache, MatrixMeta matrixMeta) {
    List<IndexPartGetRowResult> partResults = cache.getSubResponses();
    int subDim = (int) matrixMeta.getBlockColNum();
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager()
      .getPartitions(request.getMatrixId(), request.getRowId());
    int partNum = parts.size();
    IntLongVector[] subVecs = new IntLongVector[partNum];

    int[] indices = ((IntIndexGetRowRequest) request).getIndices();
    int size = partResults.size();
    for (int i = 0; i < size; i++) {
      IndexPartGetRowLongResult subResult = (IndexPartGetRowLongResult) partResults.get(i);
      long[] values = subResult.getValues();
      IntIndicesView indicesView = (IntIndicesView) subResult.getIndices();
      PartitionKey partKey = subResult.getPartKey();
      IntLongVector subVec =
        VFactory.sparseLongVector(subDim, indicesView.endPos - indicesView.startPos);
      for (int j = indicesView.startPos; j < indicesView.endPos; j++) {
        subVec.set(indices[j] - (int) partKey.getStartCol(), values[j - indicesView.startPos]);
      }
      subVecs[partKey.getPartitionId()] = subVec;
    }

    for (int i = 0; i < partNum; i++) {
      if (subVecs[i] == null) {
        subVecs[i] = VFactory.sparseLongVector(subDim, 0);
      }
    }

    CompIntLongVector vector =
      VFactory.compIntLongVector((int) matrixMeta.getColNum(), subVecs, subDim);
    vector.setMatrixId(request.getMatrixId());
    vector.setRowId(request.getRowId());
    return vector;
  }

  private static Vector combineLongDoubleIndexRowSplits(IndexGetRowRequest request,
    IndexGetRowCache cache, MatrixMeta matrixMeta) {
    List<IndexPartGetRowResult> partResults = cache.getSubResponses();
    LongDoubleVector vector = VFactory.sparseLongKeyDoubleVector(matrixMeta.getColNum(),
      ((LongIndexGetRowRequest) request).getIndices().length);
    long[] indices = ((LongIndexGetRowRequest) request).getIndices();
    int size = partResults.size();
    for (int i = 0; i < size; i++) {
      LongIndicesView indicesView = (LongIndicesView) partResults.get(i).getIndices();
      double[] values = ((IndexPartGetRowDoubleResult) (partResults.get(i))).getValues();
      for (int j = indicesView.startPos; j < indicesView.endPos; j++) {
        vector.set(indices[j], values[j - indicesView.startPos]);
      }
    }

    return vector;
  }


  private static Vector combineCompLongDoubleIndexRowSplits(IndexGetRowRequest request,
    IndexGetRowCache cache, MatrixMeta matrixMeta) {
    List<IndexPartGetRowResult> partResults = cache.getSubResponses();
    long subDim = matrixMeta.getBlockColNum();
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager()
      .getPartitions(request.getMatrixId(), request.getRowId());
    int partNum = parts.size();
    LongDoubleVector[] subVecs = new LongDoubleVector[partNum];

    long[] indices = ((LongIndexGetRowRequest) request).getIndices();
    int size = partResults.size();
    for (int i = 0; i < size; i++) {
      IndexPartGetRowDoubleResult subResult = (IndexPartGetRowDoubleResult) partResults.get(i);
      double[] values = subResult.getValues();
      LongIndicesView indicesView = (LongIndicesView) subResult.getIndices();
      PartitionKey partKey = subResult.getPartKey();
      LongDoubleVector subVec =
        VFactory.sparseLongKeyDoubleVector(subDim, indicesView.endPos - indicesView.startPos);
      for (int j = indicesView.startPos; j < indicesView.endPos; j++) {
        subVec.set(indices[j] - (int) partKey.getStartCol(), values[j - indicesView.startPos]);
      }
      subVecs[partKey.getPartitionId()] = subVec;
    }

    for (int i = 0; i < partNum; i++) {
      if (subVecs[i] == null) {
        subVecs[i] = VFactory.sparseLongKeyDoubleVector(subDim, 0);
      }
    }

    CompLongDoubleVector vector =
      VFactory.compLongDoubleVector(matrixMeta.getColNum(), subVecs, subDim);
    vector.setMatrixId(request.getMatrixId());
    vector.setRowId(request.getRowId());
    return vector;
  }

  private static Vector combineLongFloatIndexRowSplits(IndexGetRowRequest request,
    IndexGetRowCache cache, MatrixMeta matrixMeta) {
    List<IndexPartGetRowResult> partResults = cache.getSubResponses();
    LongFloatVector vector = VFactory.sparseLongKeyFloatVector(matrixMeta.getColNum(),
      ((LongIndexGetRowRequest) request).getIndices().length);
    long[] indices = ((LongIndexGetRowRequest) request).getIndices();
    int size = partResults.size();
    for (int i = 0; i < size; i++) {
      LongIndicesView indicesView = (LongIndicesView) partResults.get(i).getIndices();
      float[] values = ((IndexPartGetRowFloatResult) (partResults.get(i))).getValues();
      for (int j = indicesView.startPos; j < indicesView.endPos; j++) {
        vector.set(indices[j], values[j - indicesView.startPos]);
      }
    }

    return vector;
  }

  private static Vector combineCompLongFloatIndexRowSplits(IndexGetRowRequest request,
    IndexGetRowCache cache, MatrixMeta matrixMeta) {
    List<IndexPartGetRowResult> partResults = cache.getSubResponses();
    long subDim = matrixMeta.getBlockColNum();
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager()
      .getPartitions(request.getMatrixId(), request.getRowId());
    int partNum = parts.size();
    LongFloatVector[] subVecs = new LongFloatVector[partNum];

    long[] indices = ((LongIndexGetRowRequest) request).getIndices();
    int size = partResults.size();
    for (int i = 0; i < size; i++) {
      IndexPartGetRowFloatResult subResult = (IndexPartGetRowFloatResult) partResults.get(i);
      float[] values = subResult.getValues();
      LongIndicesView indicesView = (LongIndicesView) subResult.getIndices();
      PartitionKey partKey = subResult.getPartKey();
      LongFloatVector subVec =
        VFactory.sparseLongKeyFloatVector(subDim, indicesView.endPos - indicesView.startPos);
      for (int j = indicesView.startPos; j < indicesView.endPos; j++) {
        subVec.set(indices[j] - (int) partKey.getStartCol(), values[j - indicesView.startPos]);
      }
      subVecs[partKey.getPartitionId()] = subVec;
    }

    for (int i = 0; i < partNum; i++) {
      if (subVecs[i] == null) {
        subVecs[i] = VFactory.sparseLongKeyFloatVector(subDim, 0);
      }
    }

    CompLongFloatVector vector =
      VFactory.compLongFloatVector(matrixMeta.getColNum(), subVecs, subDim);
    vector.setMatrixId(request.getMatrixId());
    vector.setRowId(request.getRowId());
    return vector;
  }

  private static Vector combineLongIntIndexRowSplits(IndexGetRowRequest request,
    IndexGetRowCache cache, MatrixMeta matrixMeta) {
    List<IndexPartGetRowResult> partResults = cache.getSubResponses();
    LongIntVector vector = VFactory.sparseLongKeyIntVector(matrixMeta.getColNum(),
      ((LongIndexGetRowRequest) request).getIndices().length);
    long[] indices = ((LongIndexGetRowRequest) request).getIndices();
    int size = partResults.size();
    for (int i = 0; i < size; i++) {
      LongIndicesView indicesView = (LongIndicesView) partResults.get(i).getIndices();
      int[] values = ((IndexPartGetRowIntResult) (partResults.get(i))).getValues();
      for (int j = indicesView.startPos; j < indicesView.endPos; j++) {
        vector.set(indices[j], values[j - indicesView.startPos]);
      }
    }

    return vector;
  }

  private static Vector combineCompLongIntIndexRowSplits(IndexGetRowRequest request,
    IndexGetRowCache cache, MatrixMeta matrixMeta) {
    List<IndexPartGetRowResult> partResults = cache.getSubResponses();
    long subDim = matrixMeta.getBlockColNum();
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager()
      .getPartitions(request.getMatrixId(), request.getRowId());
    int partNum = parts.size();
    LongIntVector[] subVecs = new LongIntVector[partNum];

    long[] indices = ((LongIndexGetRowRequest) request).getIndices();
    int size = partResults.size();
    for (int i = 0; i < size; i++) {
      IndexPartGetRowIntResult subResult = (IndexPartGetRowIntResult) partResults.get(i);
      int[] values = subResult.getValues();
      LongIndicesView indicesView = (LongIndicesView) subResult.getIndices();
      PartitionKey partKey = subResult.getPartKey();
      LongIntVector subVec =
        VFactory.sparseLongKeyIntVector(subDim, indicesView.endPos - indicesView.startPos);
      for (int j = indicesView.startPos; j < indicesView.endPos; j++) {
        subVec.set(indices[j] - (int) partKey.getStartCol(), values[j - indicesView.startPos]);
      }
      subVecs[partKey.getPartitionId()] = subVec;
    }

    for (int i = 0; i < partNum; i++) {
      if (subVecs[i] == null) {
        subVecs[i] = VFactory.sparseLongKeyIntVector(subDim, 0);
      }
    }

    CompLongIntVector vector = VFactory.compLongIntVector(matrixMeta.getColNum(), subVecs, subDim);
    vector.setMatrixId(request.getMatrixId());
    vector.setRowId(request.getRowId());
    return vector;
  }

  private static Vector combineLongLongIndexRowSplits(IndexGetRowRequest request,
    IndexGetRowCache cache, MatrixMeta matrixMeta) {
    List<IndexPartGetRowResult> partResults = cache.getSubResponses();
    LongLongVector vector = VFactory.sparseLongKeyLongVector(matrixMeta.getColNum(),
      ((LongIndexGetRowRequest) request).getIndices().length);
    long[] indices = ((LongIndexGetRowRequest) request).getIndices();
    int size = partResults.size();
    for (int i = 0; i < size; i++) {
      LongIndicesView indicesView = (LongIndicesView) partResults.get(i).getIndices();
      long[] values = ((IndexPartGetRowLongResult) (partResults.get(i))).getValues();
      for (int j = indicesView.startPos; j < indicesView.endPos; j++) {
        vector.set(indices[j], values[j - indicesView.startPos]);
      }
    }

    return vector;
  }

  private static Vector combineCompLongLongIndexRowSplits(IndexGetRowRequest request,
    IndexGetRowCache cache, MatrixMeta matrixMeta) {
    List<IndexPartGetRowResult> partResults = cache.getSubResponses();
    long subDim = matrixMeta.getBlockColNum();
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager()
      .getPartitions(request.getMatrixId(), request.getRowId());
    int partNum = parts.size();
    LongLongVector[] subVecs = new LongLongVector[partNum];

    long[] indices = ((LongIndexGetRowRequest) request).getIndices();
    int size = partResults.size();
    for (int i = 0; i < size; i++) {
      IndexPartGetRowLongResult subResult = (IndexPartGetRowLongResult) partResults.get(i);
      long[] values = subResult.getValues();
      LongIndicesView indicesView = (LongIndicesView) subResult.getIndices();
      PartitionKey partKey = subResult.getPartKey();
      LongLongVector subVec =
        VFactory.sparseLongKeyLongVector(subDim, indicesView.endPos - indicesView.startPos);
      for (int j = indicesView.startPos; j < indicesView.endPos; j++) {
        subVec.set(indices[j] - (int) partKey.getStartCol(), values[j - indicesView.startPos]);
      }
      subVecs[partKey.getPartitionId()] = subVec;
    }

    for (int i = 0; i < partNum; i++) {
      if (subVecs[i] == null) {
        subVecs[i] = VFactory.sparseLongKeyLongVector(subDim, 0);
      }
    }

    CompLongLongVector vector =
      VFactory.compLongLongVector(matrixMeta.getColNum(), subVecs, subDim);
    vector.setMatrixId(request.getMatrixId());
    vector.setRowId(request.getRowId());
    return vector;
  }

  /**
   * Combine the row splits use pipeline mode.
   *
   * @param cache    the result cache for GET_ROW sub-requests
   * @param matrixId matrix id
   * @param rowIndex row index
   * @return Vector merged row
   * @throws InterruptedException interrupted while waiting for row splits
   */
  public static Vector combineRowSplitsPipeline(GetRowPipelineCache cache, int matrixId,
    int rowIndex) throws Exception {
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    RowType rowType = matrixMeta.getRowType();

    switch (rowType) {
      case T_DOUBLE_DENSE:
        return combineServerDenseDoubleRowSplitsPipeline(cache, matrixMeta, rowIndex);
      case T_FLOAT_DENSE:
        return combineServerDenseFloatRowSplitsPipeline(cache, matrixMeta, rowIndex);
      case T_INT_DENSE:
        return combineServerDenseIntRowSplitsPipeline(cache, matrixMeta, rowIndex);
      case T_LONG_DENSE:
        return combineServerDenseLongRowSplitsPipeline(cache, matrixMeta, rowIndex);

      default:
        return combineServerRowSplits(getAllRowSplitsFromCache(cache), matrixId, rowIndex);
    }
  }

  private static Vector combineServerIntDoubleRowSplits(List<ServerRow> rowSplits,
    MatrixMeta matrixMeta, int rowIndex) {
    int colNum = (int) matrixMeta.getColNum();
    int elemNum = 0;
    int size = rowSplits.size();
    for (int i = 0; i < size; i++) {
      elemNum += rowSplits.get(i).size();
    }

    IntDoubleVector row;
    if (elemNum >= (int) (storageConvFactor * colNum)) {
      row = VFactory.denseDoubleVector(colNum);
    } else {
      row = VFactory.sparseDoubleVector(colNum, elemNum);
    }
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);

    Collections.sort(rowSplits, serverRowComp);

    int clock = Integer.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      if (rowSplits.get(i) == null) {
        continue;
      }
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
      ((ServerIntDoubleRow) rowSplits.get(i)).mergeTo(row);
    }
    row.setClock(clock);
    return row;
  }

  private static Vector combineCompServerIntDoubleRowSplits(List<ServerRow> rowSplits,
    MatrixMeta matrixMeta, int rowIndex) {
    List<PartitionKey> partitionKeys =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixMeta.getId(), rowIndex);
    assert rowSplits.size() == partitionKeys.size();
    Collections.sort(rowSplits, serverRowComp);
    Collections.sort(partitionKeys, partKeyComp);
    IntDoubleVector[] splits = new IntDoubleVector[rowSplits.size()];

    int size = rowSplits.size();
    int clock = Integer.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      splits[i] = (IntDoubleVector) rowSplits.get(i).getSplit();
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
    }
    CompIntDoubleVector row = VFactory
      .compIntDoubleVector((int) matrixMeta.getColNum(), splits, (int) matrixMeta.getBlockColNum());
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);
    row.setClock(clock);
    return row;
  }

  private static Vector combineServerIntFloatRowSplits(List<ServerRow> rowSplits,
    MatrixMeta matrixMeta, int rowIndex) {
    int colNum = (int) matrixMeta.getColNum();
    int elemNum = 0;
    int size = rowSplits.size();
    for (int i = 0; i < size; i++) {
      elemNum += rowSplits.get(i).size();
    }

    IntFloatVector row;
    if (elemNum >= (int) (storageConvFactor * colNum)) {
      row = VFactory.denseFloatVector(colNum);
    } else {
      row = VFactory.sparseFloatVector(colNum, elemNum);
    }
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);

    Collections.sort(rowSplits, serverRowComp);

    int clock = Integer.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      if (rowSplits.get(i) == null) {
        continue;
      }
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
      ((ServerIntFloatRow) rowSplits.get(i)).mergeTo(row);
    }
    row.setClock(clock);
    return row;
  }

  private static Vector combineCompServerIntFloatRowSplits(List<ServerRow> rowSplits,
    MatrixMeta matrixMeta, int rowIndex) {
    List<PartitionKey> partitionKeys =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixMeta.getId(), rowIndex);
    assert rowSplits.size() == partitionKeys.size();
    Collections.sort(rowSplits, serverRowComp);
    Collections.sort(partitionKeys, partKeyComp);
    IntFloatVector[] splits = new IntFloatVector[rowSplits.size()];

    int size = rowSplits.size();
    int clock = Integer.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      splits[i] = (IntFloatVector) rowSplits.get(i).getSplit();
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
    }
    CompIntFloatVector row = VFactory
      .compIntFloatVector((int) matrixMeta.getColNum(), splits, (int) matrixMeta.getBlockColNum());
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);
    row.setClock(clock);
    return row;
  }

  private static Vector combineServerIntIntRowSplits(List<ServerRow> rowSplits,
    MatrixMeta matrixMeta, int rowIndex) {
    int colNum = (int) matrixMeta.getColNum();
    int elemNum = 0;
    int size = rowSplits.size();
    for (int i = 0; i < size; i++) {
      elemNum += rowSplits.get(i).size();
    }

    IntIntVector row;
    if (elemNum >= (int) (storageConvFactor * colNum)) {
      row = VFactory.denseIntVector(colNum);
    } else {
      row = VFactory.sparseIntVector(colNum, elemNum);
    }
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);

    Collections.sort(rowSplits, serverRowComp);

    int clock = Integer.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      if (rowSplits.get(i) == null) {
        continue;
      }
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
      ((ServerIntIntRow) rowSplits.get(i)).mergeTo(row);
    }
    row.setClock(clock);
    return row;
  }

  private static Vector combineCompServerIntIntRowSplits(List<ServerRow> rowSplits,
    MatrixMeta matrixMeta, int rowIndex) {
    List<PartitionKey> partitionKeys =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixMeta.getId(), rowIndex);
    assert rowSplits.size() == partitionKeys.size();
    Collections.sort(rowSplits, serverRowComp);
    Collections.sort(partitionKeys, partKeyComp);
    IntIntVector[] splits = new IntIntVector[rowSplits.size()];

    int size = rowSplits.size();
    int clock = Integer.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      splits[i] = (IntIntVector) rowSplits.get(i).getSplit();
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
    }
    CompIntIntVector row = VFactory
      .compIntIntVector((int) matrixMeta.getColNum(), splits, (int) matrixMeta.getBlockColNum());
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);
    row.setClock(clock);
    return row;
  }

  private static Vector combineServerIntLongRowSplits(List<ServerRow> rowSplits,
    MatrixMeta matrixMeta, int rowIndex) {
    int colNum = (int) matrixMeta.getColNum();
    int elemNum = 0;
    int size = rowSplits.size();
    for (int i = 0; i < size; i++) {
      elemNum += rowSplits.get(i).size();
    }

    IntLongVector row;
    if (elemNum >= (int) (storageConvFactor * colNum)) {
      row = VFactory.denseLongVector(colNum);
    } else {
      row = VFactory.sparseLongVector(colNum, elemNum);
    }
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);

    Collections.sort(rowSplits, serverRowComp);

    int clock = Integer.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      if (rowSplits.get(i) == null) {
        continue;
      }
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
      ((ServerIntLongRow) rowSplits.get(i)).mergeTo(row);
    }
    row.setClock(clock);
    return row;
  }

  private static Vector combineCompServerIntLongRowSplits(List<ServerRow> rowSplits,
    MatrixMeta matrixMeta, int rowIndex) {
    List<PartitionKey> partitionKeys =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixMeta.getId(), rowIndex);
    assert rowSplits.size() == partitionKeys.size();
    Collections.sort(rowSplits, serverRowComp);
    Collections.sort(partitionKeys, partKeyComp);
    IntLongVector[] splits = new IntLongVector[rowSplits.size()];

    int size = rowSplits.size();
    int clock = Integer.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      splits[i] = (IntLongVector) rowSplits.get(i).getSplit();
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
    }
    CompIntLongVector row = VFactory
      .compIntLongVector((int) matrixMeta.getColNum(), splits, (int) matrixMeta.getBlockColNum());
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);
    row.setClock(clock);
    return row;
  }

  private static Vector combineServerLongDoubleRowSplits(List<ServerRow> rowSplits,
    MatrixMeta matrixMeta, int rowIndex) {
    long colNum = matrixMeta.getColNum();
    int elemNum = 0;
    int size = rowSplits.size();
    for (int i = 0; i < size; i++) {
      elemNum += rowSplits.get(i).size();
    }

    LongDoubleVector row = VFactory.sparseLongKeyDoubleVector(colNum, elemNum);
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);

    Collections.sort(rowSplits, serverRowComp);

    int clock = Integer.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      if (rowSplits.get(i) == null) {
        continue;
      }
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
      ((ServerLongDoubleRow) rowSplits.get(i)).mergeTo(row);
    }
    row.setClock(clock);
    return row;
  }

  private static Vector combineCompServerLongDoubleRowSplits(List<ServerRow> rowSplits,
    MatrixMeta matrixMeta, int rowIndex) {
    List<PartitionKey> partitionKeys =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixMeta.getId(), rowIndex);
    assert rowSplits.size() == partitionKeys.size();
    Collections.sort(rowSplits, serverRowComp);
    Collections.sort(partitionKeys, partKeyComp);
    LongDoubleVector[] splits = new LongDoubleVector[rowSplits.size()];

    int size = rowSplits.size();
    int clock = Integer.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      splits[i] = (LongDoubleVector) rowSplits.get(i).getSplit();
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
    }
    CompLongDoubleVector row =
      VFactory.compLongDoubleVector(matrixMeta.getColNum(), splits, matrixMeta.getBlockColNum());
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);
    row.setClock(clock);
    return row;
  }

  private static Vector combineServerLongFloatRowSplits(List<ServerRow> rowSplits,
    MatrixMeta matrixMeta, int rowIndex) {
    long colNum = matrixMeta.getColNum();
    int elemNum = 0;
    int size = rowSplits.size();
    for (int i = 0; i < size; i++) {
      elemNum += rowSplits.get(i).size();
    }

    LongFloatVector row = VFactory.sparseLongKeyFloatVector(colNum, elemNum);
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);

    Collections.sort(rowSplits, serverRowComp);

    int clock = Integer.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      if (rowSplits.get(i) == null) {
        continue;
      }
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
      ((ServerLongFloatRow) rowSplits.get(i)).mergeTo(row);
    }
    row.setClock(clock);
    return row;
  }

  private static Vector combineCompServerLongFloatRowSplits(List<ServerRow> rowSplits,
    MatrixMeta matrixMeta, int rowIndex) {
    List<PartitionKey> partitionKeys =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixMeta.getId(), rowIndex);
    assert rowSplits.size() == partitionKeys.size();
    Collections.sort(rowSplits, serverRowComp);
    Collections.sort(partitionKeys, partKeyComp);
    LongFloatVector[] splits = new LongFloatVector[rowSplits.size()];

    int size = rowSplits.size();
    int clock = Integer.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      splits[i] = (LongFloatVector) rowSplits.get(i).getSplit();
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
    }
    CompLongFloatVector row =
      VFactory.compLongFloatVector(matrixMeta.getColNum(), splits, matrixMeta.getBlockColNum());
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);
    row.setClock(clock);
    return row;
  }

  private static Vector combineServerLongIntRowSplits(List<ServerRow> rowSplits,
    MatrixMeta matrixMeta, int rowIndex) {
    long colNum = matrixMeta.getColNum();
    int elemNum = 0;
    int size = rowSplits.size();
    for (int i = 0; i < size; i++) {
      elemNum += rowSplits.get(i).size();
    }

    LongIntVector row = VFactory.sparseLongKeyIntVector(colNum, elemNum);
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);

    Collections.sort(rowSplits, serverRowComp);

    int clock = Integer.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      if (rowSplits.get(i) == null) {
        continue;
      }
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
      ((ServerLongIntRow) rowSplits.get(i)).mergeTo(row);
    }
    row.setClock(clock);
    return row;
  }

  private static Vector combineCompServerLongIntRowSplits(List<ServerRow> rowSplits,
    MatrixMeta matrixMeta, int rowIndex) {
    List<PartitionKey> partitionKeys =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixMeta.getId(), rowIndex);
    assert rowSplits.size() == partitionKeys.size();
    Collections.sort(rowSplits, serverRowComp);
    Collections.sort(partitionKeys, partKeyComp);
    LongIntVector[] splits = new LongIntVector[rowSplits.size()];

    int size = rowSplits.size();
    int clock = Integer.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      splits[i] = (LongIntVector) rowSplits.get(i).getSplit();
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
    }
    CompLongIntVector row =
      VFactory.compLongIntVector(matrixMeta.getColNum(), splits, matrixMeta.getBlockColNum());
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);
    row.setClock(clock);
    return row;
  }

  private static Vector combineServerLongLongRowSplits(List<ServerRow> rowSplits,
    MatrixMeta matrixMeta, int rowIndex) {
    long colNum = matrixMeta.getColNum();
    int elemNum = 0;
    int size = rowSplits.size();
    for (int i = 0; i < size; i++) {
      elemNum += rowSplits.get(i).size();
    }

    LongLongVector row = VFactory.sparseLongKeyLongVector(colNum, elemNum);
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);

    Collections.sort(rowSplits, serverRowComp);

    int clock = Integer.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      if (rowSplits.get(i) == null) {
        continue;
      }
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
      ((ServerLongLongRow) rowSplits.get(i)).mergeTo(row);
    }
    row.setClock(clock);
    return row;
  }

  private static Vector combineCompServerLongLongRowSplits(List<ServerRow> rowSplits,
    MatrixMeta matrixMeta, int rowIndex) {
    List<PartitionKey> partitionKeys =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixMeta.getId(), rowIndex);
    assert rowSplits.size() == partitionKeys.size();
    Collections.sort(rowSplits, serverRowComp);
    Collections.sort(partitionKeys, partKeyComp);
    LongLongVector[] splits = new LongLongVector[rowSplits.size()];

    int size = rowSplits.size();
    int clock = Integer.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      splits[i] = (LongLongVector) rowSplits.get(i).getSplit();
      if (rowSplits.get(i).getClock() < clock) {
        clock = rowSplits.get(i).getClock();
      }
    }
    CompLongLongVector row =
      VFactory.compLongLongVector(matrixMeta.getColNum(), splits, matrixMeta.getBlockColNum());
    row.setMatrixId(matrixMeta.getId());
    row.setRowId(rowIndex);
    row.setClock(clock);
    return row;
  }



  private static List<ServerRow> getAllRowSplitsFromCache(GetRowPipelineCache cache)
    throws InterruptedException {
    int splitNum = cache.getTotalRequestNum();
    List<ServerRow> splits = new ArrayList<ServerRow>(splitNum);
    ServerRow split = null;

    int index = 0;
    while ((split = cache.readNextSubResponse()) != null && index < splitNum) {
      splits.add(split);
      index++;
    }

    return splits;
  }

  private static Vector combineServerDenseDoubleRowSplitsPipeline(GetRowPipelineCache pipelineCache,
    MatrixMeta matrixMeta, int rowIndex) throws InterruptedException {
    int colNum = (int) matrixMeta.getColNum();
    int clock = Integer.MAX_VALUE;
    IntDoubleVector row = VFactory.denseDoubleVector(colNum);
    while (true) {
      ServerRow split = pipelineCache.readNextSubResponse();
      if (split == null) {
        row.setMatrixId(matrixMeta.getId());
        row.setRowId(rowIndex);
        row.setClock(clock);
        return row;
      }

      if (split.getClock() < clock) {
        clock = split.getClock();
      }
      ((ServerIntDoubleRow) split).mergeTo(row);
    }
  }

  private static Vector combineServerDenseFloatRowSplitsPipeline(GetRowPipelineCache pipelineCache,
    MatrixMeta matrixMeta, int rowIndex) throws InterruptedException {
    int colNum = (int) matrixMeta.getColNum();
    int clock = Integer.MAX_VALUE;
    IntFloatVector row = VFactory.denseFloatVector(colNum);
    while (true) {
      ServerRow split = pipelineCache.readNextSubResponse();
      if (split == null) {
        row.setMatrixId(matrixMeta.getId());
        row.setRowId(rowIndex);
        row.setClock(clock);
        return row;
      }

      if (split.getClock() < clock) {
        clock = split.getClock();
      }
      ((ServerIntFloatRow) split).mergeTo(row);
    }
  }

  private static Vector combineServerDenseIntRowSplitsPipeline(GetRowPipelineCache pipelineCache,
    MatrixMeta matrixMeta, int rowIndex) throws InterruptedException {
    int colNum = (int) matrixMeta.getColNum();
    int clock = Integer.MAX_VALUE;
    IntIntVector row = VFactory.denseIntVector(colNum);
    while (true) {
      ServerRow split = pipelineCache.readNextSubResponse();
      if (split == null) {
        row.setMatrixId(matrixMeta.getId());
        row.setRowId(rowIndex);
        row.setClock(clock);
        return row;
      }

      if (split.getClock() < clock) {
        clock = split.getClock();
      }
      ((ServerIntIntRow) split).mergeTo(row);
    }
  }

  private static Vector combineServerDenseLongRowSplitsPipeline(GetRowPipelineCache pipelineCache,
    MatrixMeta matrixMeta, int rowIndex) throws InterruptedException {
    int colNum = (int) matrixMeta.getColNum();
    int clock = Integer.MAX_VALUE;
    IntLongVector row = VFactory.denseLongVector(colNum);
    while (true) {
      ServerRow split = pipelineCache.readNextSubResponse();
      if (split == null) {
        row.setMatrixId(matrixMeta.getId());
        row.setRowId(rowIndex);
        row.setClock(clock);
        return row;
      }

      if (split.getClock() < clock) {
        clock = split.getClock();
      }
      ((ServerIntLongRow) split).mergeTo(row);
    }
  }
}
