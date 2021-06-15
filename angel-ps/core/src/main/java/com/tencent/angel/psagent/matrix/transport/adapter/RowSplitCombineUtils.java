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
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.IntLongVector;
import com.tencent.angel.ml.math2.vector.LongDoubleVector;
import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.math2.vector.LongIntVector;
import com.tencent.angel.ml.math2.vector.LongLongVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.vector.ServerIntDoubleRow;
import com.tencent.angel.ps.storage.vector.ServerIntFloatRow;
import com.tencent.angel.ps.storage.vector.ServerIntIntRow;
import com.tencent.angel.ps.storage.vector.ServerIntLongRow;
import com.tencent.angel.ps.storage.vector.ServerLongDoubleRow;
import com.tencent.angel.ps.storage.vector.ServerLongFloatRow;
import com.tencent.angel.ps.storage.vector.ServerLongIntRow;
import com.tencent.angel.ps.storage.vector.ServerLongLongRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.ValuePart;
import com.tencent.angel.psagent.matrix.transport.router.hash.HashIntKeysPart;
import com.tencent.angel.psagent.matrix.transport.router.hash.HashLongKeysPart;
import com.tencent.angel.psagent.matrix.transport.router.range.RangeViewIntKeysPart;
import com.tencent.angel.psagent.matrix.transport.router.range.RangeViewLongKeysPart;
import com.tencent.angel.psagent.matrix.transport.router.value.DoubleValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.value.FloatValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.value.IntValuesPart;
import com.tencent.angel.psagent.matrix.transport.router.value.LongValuesPart;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Row splits combine tool.
 */
public class RowSplitCombineUtils {

  private static final Log LOG = LogFactory.getLog(RowSplitCombineUtils.class);
  private static final Comparator serverRowComp = new StartColComparator();
  private static final Comparator partKeyComp = new PartitionKeyComparator();
  private static final float storageConvFactor = 0.25f;

  static class StartColComparator implements Comparator<ServerRow> {

    @Override
    public int compare(ServerRow r1, ServerRow r2) {
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

    @Override
    public int compare(PartitionKey p1, PartitionKey p2) {
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
   * @param matrixId matrix id
   * @param rowIndex row index
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

      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE:
        return combineServerIntFloatRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_INT_DENSE:
      case T_INT_SPARSE:
        return combineServerIntIntRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_LONG_DENSE:
      case T_LONG_SPARSE:
        return combineServerIntLongRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_DOUBLE_SPARSE_LONGKEY:
        return combineServerLongDoubleRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_FLOAT_SPARSE_LONGKEY:
        return combineServerLongFloatRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_INT_SPARSE_LONGKEY:
        return combineServerLongIntRowSplits(rowSplits, matrixMeta, rowIndex);

      case T_LONG_SPARSE_LONGKEY:
        return combineServerLongLongRowSplits(rowSplits, matrixMeta, rowIndex);

      default:
        throw new UnsupportedOperationException(
            "Unsupport operation: merge " + rowType + " vector splits");
    }
  }

  /**
   * Combine the rows splits.
   *
   * @param matrixId Matrix id
   * @param rowIds row id
   * @param resultSize keys number
   * @param keyParts keys partitions
   * @param valueParts values partitions
   * @return merged vectors
   */
  public static Vector[] combineIndexRowsSplits(int matrixId, int[] rowIds, int resultSize,
      KeyPart[] keyParts, ValuePart[][] valueParts) {
    Vector[] vectors = new Vector[rowIds.length];
    for (int i = 0; i < rowIds.length; i++) {
      vectors[i] = combineIndexRowSplits(matrixId, rowIds[i], resultSize, keyParts, valueParts[i]);
    }
    return vectors;
  }

  /**
   * Combine the row splits.
   *
   * @param matrixId Matrix id
   * @param rowId row id
   * @param resultSize keys number
   * @param keyParts keys partitions
   * @param valueParts values partitions
   * @return merged vector
   */
  public static Vector combineIndexRowSplits(int matrixId, int rowId, int resultSize,
      KeyPart[] keyParts, ValuePart[] valueParts) {

    // Get matrix meta
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    RowType rowType = matrixMeta.getRowType();

    switch (rowType) {
      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE:
        return combineIntDoubleIndexRowSplits(matrixId, rowId, resultSize, keyParts, valueParts,
            matrixMeta);

      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE:
        return combineIntFloatIndexRowSplits(matrixId, rowId, resultSize, keyParts, valueParts,
            matrixMeta);

      case T_INT_DENSE:
      case T_INT_SPARSE:
        return combineIntIntIndexRowSplits(matrixId, rowId, resultSize, keyParts, valueParts,
            matrixMeta);

      case T_LONG_DENSE:
      case T_LONG_SPARSE:
        return combineIntLongIndexRowSplits(matrixId, rowId, resultSize, keyParts, valueParts,
            matrixMeta);

      case T_DOUBLE_SPARSE_LONGKEY:
        return combineLongDoubleIndexRowSplits(matrixId, rowId, resultSize, keyParts, valueParts,
            matrixMeta);

      case T_FLOAT_SPARSE_LONGKEY:
        return combineLongFloatIndexRowSplits(matrixId, rowId, resultSize, keyParts, valueParts,
            matrixMeta);

      case T_INT_SPARSE_LONGKEY:
        return combineLongIntIndexRowSplits(matrixId, rowId, resultSize, keyParts, valueParts,
            matrixMeta);

      case T_LONG_SPARSE_LONGKEY:
        return combineLongLongIndexRowSplits(matrixId, rowId, resultSize, keyParts, valueParts,
            matrixMeta);

      default:
        throw new UnsupportedOperationException("unsupport row type " + rowType);
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Combine Int key Double value vector
  ////////////////////////////////////////////////////////////////////////////////
  public static Vector combineIntDoubleIndexRowSplits(int matrixId, int rowId, int resultSize,
      KeyPart[] keyParts, ValuePart[] valueParts,
      MatrixMeta matrixMeta) {
    IntDoubleVector vector = VFactory.sparseDoubleVector((int) matrixMeta.getColNum(), resultSize);
    for (int i = 0; i < keyParts.length; i++) {
      mergeTo(vector, keyParts[i], (DoubleValuesPart) valueParts[i]);
    }
    vector.setRowId(rowId);
    vector.setMatrixId(matrixId);
    return vector;
  }

  public static void mergeTo(IntDoubleVector vector, KeyPart keysPart,
      DoubleValuesPart valuesPart) {
    if (keysPart instanceof RangeViewIntKeysPart) {
      mergeTo(vector, (RangeViewIntKeysPart) keysPart, valuesPart);
    } else {
      mergeTo(vector, (HashIntKeysPart) keysPart, valuesPart);
    }
  }

  public static void mergeTo(IntDoubleVector vector, RangeViewIntKeysPart keysPart,
      DoubleValuesPart valuesPart) {
    int[] keys = keysPart.getKeys();
    int startPos = keysPart.getStartPos();
    int endPos = keysPart.getEndPos();
    double[] values = valuesPart.getValues();
    for (int i = startPos; i < endPos; i++) {
      vector.set(keys[i], values[i - startPos]);
    }
  }

  public static void mergeTo(IntDoubleVector vector, HashIntKeysPart keysPart,
      DoubleValuesPart valuesPart) {
    int[] keys = keysPart.getKeys();
    double[] values = valuesPart.getValues();
    for (int i = 0; i < keys.length; i++) {
      vector.set(keys[i], values[i]);
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Combine Int key Float value vector
  ////////////////////////////////////////////////////////////////////////////////
  public static Vector combineIntFloatIndexRowSplits(int matrixId, int rowId, int resultSize,
      KeyPart[] keyParts, ValuePart[] valueParts,
      MatrixMeta matrixMeta) {
    IntFloatVector vector = VFactory.sparseFloatVector((int) matrixMeta.getColNum(), resultSize);
    for (int i = 0; i < keyParts.length; i++) {
      mergeTo(vector, keyParts[i], (FloatValuesPart) valueParts[i]);
    }
    vector.setRowId(rowId);
    vector.setMatrixId(matrixId);
    return vector;
  }

  public static void mergeTo(IntFloatVector vector, KeyPart keysPart, FloatValuesPart valuesPart) {
    if (keysPart instanceof RangeViewIntKeysPart) {
      mergeTo(vector, (RangeViewIntKeysPart) keysPart, valuesPart);
    } else {
      mergeTo(vector, (HashIntKeysPart) keysPart, valuesPart);
    }
  }

  public static void mergeTo(IntFloatVector vector, RangeViewIntKeysPart keysPart,
      FloatValuesPart valuesPart) {
    int[] keys = keysPart.getKeys();
    int startPos = keysPart.getStartPos();
    int endPos = keysPart.getEndPos();
    float[] values = valuesPart.getValues();
    for (int i = startPos; i < endPos; i++) {
      vector.set(keys[i], values[i - startPos]);
    }
  }

  public static void mergeTo(IntFloatVector vector, HashIntKeysPart keysPart,
      FloatValuesPart valuesPart) {
    int[] keys = keysPart.getKeys();
    float[] values = valuesPart.getValues();
    for (int i = 0; i < keys.length; i++) {
      vector.set(keys[i], values[i]);
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Combine Int key Long value vector
  ////////////////////////////////////////////////////////////////////////////////
  public static Vector combineIntLongIndexRowSplits(int matrixId, int rowId, int resultSize,
      KeyPart[] keyParts, ValuePart[] valueParts,
      MatrixMeta matrixMeta) {
    IntLongVector vector = VFactory.sparseLongVector((int) matrixMeta.getColNum(), resultSize);
    for (int i = 0; i < keyParts.length; i++) {
      mergeTo(vector, keyParts[i], (LongValuesPart) valueParts[i]);
    }
    vector.setRowId(rowId);
    vector.setMatrixId(matrixId);
    return vector;
  }

  public static void mergeTo(IntLongVector vector, KeyPart keysPart, LongValuesPart valuesPart) {
    if (keysPart instanceof RangeViewIntKeysPart) {
      mergeTo(vector, (RangeViewIntKeysPart) keysPart, valuesPart);
    } else {
      mergeTo(vector, (HashIntKeysPart) keysPart, valuesPart);
    }
  }

  public static void mergeTo(IntLongVector vector, RangeViewIntKeysPart keysPart,
      LongValuesPart valuesPart) {
    int[] keys = keysPart.getKeys();
    int startPos = keysPart.getStartPos();
    int endPos = keysPart.getEndPos();
    long[] values = valuesPart.getValues();
    for (int i = startPos; i < endPos; i++) {
      vector.set(keys[i], values[i - startPos]);
    }
  }

  public static void mergeTo(IntLongVector vector, HashIntKeysPart keysPart,
      LongValuesPart valuesPart) {
    int[] keys = keysPart.getKeys();
    long[] values = valuesPart.getValues();
    for (int i = 0; i < keys.length; i++) {
      vector.set(keys[i], values[i]);
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Combine Int key Int value vector
  ////////////////////////////////////////////////////////////////////////////////
  public static Vector combineIntIntIndexRowSplits(int matrixId, int rowId, int resultSize,
      KeyPart[] keyParts, ValuePart[] valueParts,
      MatrixMeta matrixMeta) {
    IntIntVector vector = VFactory.sparseIntVector((int) matrixMeta.getColNum(), resultSize);
    for (int i = 0; i < keyParts.length; i++) {
      mergeTo(vector, keyParts[i], (IntValuesPart) valueParts[i]);
    }
    vector.setRowId(rowId);
    vector.setMatrixId(matrixId);
    return vector;
  }

  public static void mergeTo(IntIntVector vector, KeyPart keysPart, IntValuesPart valuesPart) {
    if (keysPart instanceof RangeViewIntKeysPart) {
      mergeTo(vector, (RangeViewIntKeysPart) keysPart, valuesPart);
    } else {
      mergeTo(vector, (HashIntKeysPart) keysPart, valuesPart);
    }
  }

  public static void mergeTo(IntIntVector vector, RangeViewIntKeysPart keysPart,
      IntValuesPart valuesPart) {
    int[] keys = keysPart.getKeys();
    int startPos = keysPart.getStartPos();
    int endPos = keysPart.getEndPos();
    int[] values = valuesPart.getValues();
    for (int i = startPos; i < endPos; i++) {
      vector.set(keys[i], values[i - startPos]);
    }
  }

  public static void mergeTo(IntIntVector vector, HashIntKeysPart keysPart,
      IntValuesPart valuesPart) {
    int[] keys = keysPart.getKeys();
    int[] values = valuesPart.getValues();
    for (int i = 0; i < keys.length; i++) {
      vector.set(keys[i], values[i]);
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Combine Long key Double value vector
  ////////////////////////////////////////////////////////////////////////////////
  public static Vector combineLongDoubleIndexRowSplits(int matrixId, int rowId, int resultSize,
      KeyPart[] keyParts, ValuePart[] valueParts,
      MatrixMeta matrixMeta) {
    LongDoubleVector vector = VFactory
        .sparseLongKeyDoubleVector(matrixMeta.getColNum(), resultSize);
    for (int i = 0; i < keyParts.length; i++) {
      mergeTo(vector, keyParts[i], (DoubleValuesPart) valueParts[i]);
    }
    vector.setRowId(rowId);
    vector.setMatrixId(matrixId);
    return vector;
  }

  public static void mergeTo(LongDoubleVector vector, KeyPart keysPart,
      DoubleValuesPart valuesPart) {
    if (keysPart instanceof RangeViewLongKeysPart) {
      mergeTo(vector, (RangeViewLongKeysPart) keysPart, valuesPart);
    } else {
      mergeTo(vector, (HashLongKeysPart) keysPart, valuesPart);
    }
  }

  public static void mergeTo(LongDoubleVector vector, RangeViewLongKeysPart keysPart,
      DoubleValuesPart valuesPart) {
    long[] keys = keysPart.getKeys();
    int startPos = keysPart.getStartPos();
    int endPos = keysPart.getEndPos();
    double[] values = valuesPart.getValues();
    for (int i = startPos; i < endPos; i++) {
      vector.set(keys[i], values[i - startPos]);
    }
  }

  public static void mergeTo(LongDoubleVector vector, HashLongKeysPart keysPart,
      DoubleValuesPart valuesPart) {
    long[] keys = keysPart.getKeys();
    double[] values = valuesPart.getValues();
    for (int i = 0; i < keys.length; i++) {
      vector.set(keys[i], values[i]);
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Combine Long key Float value vector
  ////////////////////////////////////////////////////////////////////////////////
  public static Vector combineLongFloatIndexRowSplits(int matrixId, int rowId, int resultSize,
      KeyPart[] keyParts, ValuePart[] valueParts,
      MatrixMeta matrixMeta) {
    LongFloatVector vector = VFactory.sparseLongKeyFloatVector(matrixMeta.getColNum(), resultSize);
    for (int i = 0; i < keyParts.length; i++) {
      mergeTo(vector, keyParts[i], (FloatValuesPart) valueParts[i]);
    }
    vector.setRowId(rowId);
    vector.setMatrixId(matrixId);
    return vector;
  }

  public static void mergeTo(LongFloatVector vector, KeyPart keysPart, FloatValuesPart valuesPart) {
    if (keysPart instanceof RangeViewLongKeysPart) {
      mergeTo(vector, (RangeViewLongKeysPart) keysPart, valuesPart);
    } else {
      mergeTo(vector, (HashLongKeysPart) keysPart, valuesPart);
    }
  }

  public static void mergeTo(LongFloatVector vector, RangeViewLongKeysPart keysPart,
      FloatValuesPart valuesPart) {
    long[] keys = keysPart.getKeys();
    int startPos = keysPart.getStartPos();
    int endPos = keysPart.getEndPos();
    float[] values = valuesPart.getValues();
    for (int i = startPos; i < endPos; i++) {
      vector.set(keys[i], values[i - startPos]);
    }
  }

  public static void mergeTo(LongFloatVector vector, HashLongKeysPart keysPart,
      FloatValuesPart valuesPart) {
    long[] keys = keysPart.getKeys();
    float[] values = valuesPart.getValues();
    for (int i = 0; i < keys.length; i++) {
      vector.set(keys[i], values[i]);
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Combine Long key Int value vector
  ////////////////////////////////////////////////////////////////////////////////
  public static Vector combineLongIntIndexRowSplits(int matrixId, int rowId, int resultSize,
      KeyPart[] keyParts, ValuePart[] valueParts,
      MatrixMeta matrixMeta) {
    LongIntVector vector = VFactory.sparseLongKeyIntVector(matrixMeta.getColNum(), resultSize);
    for (int i = 0; i < keyParts.length; i++) {
      mergeTo(vector, keyParts[i], (IntValuesPart) valueParts[i]);
    }
    vector.setRowId(rowId);
    vector.setMatrixId(matrixId);
    return vector;
  }

  public static void mergeTo(LongIntVector vector, KeyPart keysPart, IntValuesPart valuesPart) {
    if (keysPart instanceof RangeViewLongKeysPart) {
      mergeTo(vector, (RangeViewLongKeysPart) keysPart, valuesPart);
    } else {
      mergeTo(vector, (HashLongKeysPart) keysPart, valuesPart);
    }
  }

  public static void mergeTo(LongIntVector vector, RangeViewLongKeysPart keysPart,
      IntValuesPart valuesPart) {
    long[] keys = keysPart.getKeys();
    int startPos = keysPart.getStartPos();
    int endPos = keysPart.getEndPos();
    int[] values = valuesPart.getValues();
    for (int i = startPos; i < endPos; i++) {
      vector.set(keys[i], values[i - startPos]);
    }
  }

  public static void mergeTo(LongIntVector vector, HashLongKeysPart keysPart,
      IntValuesPart valuesPart) {
    long[] keys = keysPart.getKeys();
    int[] values = valuesPart.getValues();
    for (int i = 0; i < keys.length; i++) {
      vector.set(keys[i], values[i]);
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Combine Long key Long value vector
  ////////////////////////////////////////////////////////////////////////////////
  public static Vector combineLongLongIndexRowSplits(int matrixId, int rowId, int resultSize,
      KeyPart[] keyParts, ValuePart[] valueParts,
      MatrixMeta matrixMeta) {
    LongLongVector vector = VFactory.sparseLongKeyLongVector(matrixMeta.getColNum(), resultSize);
    for (int i = 0; i < keyParts.length; i++) {
      mergeTo(vector, keyParts[i], (LongValuesPart) valueParts[i]);
    }
    vector.setRowId(rowId);
    vector.setMatrixId(matrixId);
    return vector;
  }

  public static void mergeTo(LongLongVector vector, KeyPart keysPart, LongValuesPart valuesPart) {
    if (keysPart instanceof RangeViewLongKeysPart) {
      mergeTo(vector, (RangeViewLongKeysPart) keysPart, valuesPart);
    } else {
      mergeTo(vector, (HashLongKeysPart) keysPart, valuesPart);
    }
  }

  public static void mergeTo(LongLongVector vector, RangeViewLongKeysPart keysPart,
      LongValuesPart valuesPart) {
    long[] keys = keysPart.getKeys();
    int startPos = keysPart.getStartPos();
    int endPos = keysPart.getEndPos();
    long[] values = valuesPart.getValues();
    for (int i = startPos; i < endPos; i++) {
      vector.set(keys[i], values[i - startPos]);
    }
  }

  public static void mergeTo(LongLongVector vector, HashLongKeysPart keysPart,
      LongValuesPart valuesPart) {
    long[] keys = keysPart.getKeys();
    long[] values = valuesPart.getValues();
    for (int i = 0; i < keys.length; i++) {
      vector.set(keys[i], values[i]);
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
}
