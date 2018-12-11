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

package com.tencent.angel.model.io;

import com.tencent.angel.ml.math2.MFactory;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.matrix.MapMatrix;
import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.ml.math2.utils.RowType;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.IntLongVector;
import com.tencent.angel.model.MatrixLoadContext;
import com.tencent.angel.model.output.element.IntDoublesCol;
import com.tencent.angel.model.output.element.IntFloatsCol;
import com.tencent.angel.model.output.element.IntIntsCol;
import com.tencent.angel.model.output.element.IntLongsCol;
import com.tencent.angel.model.output.element.LongDoublesCol;
import com.tencent.angel.model.output.element.LongFloatsCol;
import com.tencent.angel.model.output.element.LongIntsCol;
import com.tencent.angel.model.output.element.LongLongsCol;
import com.tencent.angel.model.output.format.ColumnFormat;
import com.tencent.angel.model.output.format.MatrixFilesMeta;
import com.tencent.angel.model.output.format.MatrixPartitionMeta;
import com.tencent.angel.model.output.format.RowPartitionMeta;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

/**
 * Loader/Saver for column-base matrix
 */
public class ColumnLoaderSaver extends MatrixLoaderSaverImpl {

  /**
   * Matrix save format
   */
  private final ColumnFormat format;

  /**
   * Model type
   */
  private volatile RowType rowType;

  public ColumnLoaderSaver(ColumnFormat format, Configuration conf) {
    super(conf);
    this.format = format;
  }

  @Override
  public void load(Matrix matrix, MatrixPartitionMeta partMeta, MatrixLoadContext loadContext,
      FSDataInputStream input)
      throws IOException {
    assert rowType != null;
    Map<Integer, RowPartitionMeta> rowMetas = partMeta.getRowMetas();

    switch (rowType) {
      case T_INT_DENSE:
      case T_INT_SPARSE:
      case T_INT_DENSE_COMPONENT:
      case T_INT_SPARSE_COMPONENT:
        loadIntIntRows((MapMatrix) matrix, partMeta, loadContext, input);
        return;

      case T_LONG_DENSE:
      case T_LONG_SPARSE:
      case T_LONG_DENSE_COMPONENT:
      case T_LONG_SPARSE_COMPONENT:
        loadIntLongRows((MapMatrix) matrix, partMeta, loadContext, input);
        return;

      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE:
      case T_FLOAT_DENSE_COMPONENT:
      case T_FLOAT_SPARSE_COMPONENT:
        loadIntFloatRows((MapMatrix) matrix, partMeta, loadContext, input);
        return;

      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE:
      case T_DOUBLE_DENSE_COMPONENT:
      case T_DOUBLE_SPARSE_COMPONENT:
        loadIntDoubleRows((MapMatrix) matrix, partMeta, loadContext, input);
        return;

      case T_INT_SPARSE_LONGKEY:
      case T_INT_DENSE_LONGKEY_COMPONENT:
      case T_INT_SPARSE_LONGKEY_COMPONENT:
        loadLongIntRows((MapMatrix) matrix, partMeta, loadContext, input);
        return;

      case T_LONG_SPARSE_LONGKEY:
      case T_LONG_DENSE_LONGKEY_COMPONENT:
      case T_LONG_SPARSE_LONGKEY_COMPONENT:
        loadLongLongRows((MapMatrix) matrix, partMeta, loadContext, input);
        return;

      case T_FLOAT_SPARSE_LONGKEY:
      case T_FLOAT_DENSE_LONGKEY_COMPONENT:
      case T_FLOAT_SPARSE_LONGKEY_COMPONENT:
        loadLongFloatRows((MapMatrix) matrix, partMeta, loadContext, input);
        return;

      case T_DOUBLE_SPARSE_LONGKEY:
      case T_DOUBLE_DENSE_LONGKEY_COMPONENT:
      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
        loadLongDoubleRows((MapMatrix) matrix, partMeta, loadContext, input);
        return;

      default:
        throw new UnsupportedOperationException("Unsupport row type " + rowType);
    }
  }

  @Override
  public Matrix initMatrix(MatrixFilesMeta matrixFilesMeta) {
    Map<Integer, MatrixPartitionMeta> partMetas = matrixFilesMeta.getPartMetas();
    int featureCount = 0;
    for (MatrixPartitionMeta partMeta : partMetas.values()) {
      Map<Integer, RowPartitionMeta> rowMetas = partMeta.getRowMetas();
      assert rowMetas.containsKey(0);
      featureCount += rowMetas.get(0).getElementNum();
    }

    rowType = RowType.valueOf(matrixFilesMeta.getRowType());

    return mapRbMatrix(rowType, matrixFilesMeta.getMatrixId(), featureCount,
        matrixFilesMeta.getRow());
  }

  private static MapMatrix mapRbMatrix(RowType rowType, int matrixId, int rowNum, int colNum) {
    switch (rowType) {
      case T_INT_DENSE:
      case T_INT_SPARSE:
      case T_INT_DENSE_COMPONENT:
      case T_INT_SPARSE_COMPONENT:
        return MFactory.intIntMapMatrix(matrixId, -1, new HashMap<>(rowNum));

      case T_LONG_DENSE:
      case T_LONG_SPARSE:
      case T_LONG_DENSE_COMPONENT:
      case T_LONG_SPARSE_COMPONENT:
        return MFactory.intLongMapMatrix(matrixId, -1, new HashMap<>(rowNum));

      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE:
      case T_FLOAT_DENSE_COMPONENT:
      case T_FLOAT_SPARSE_COMPONENT:
        return MFactory.intFloatMapMatrix(matrixId, -1, new HashMap<>(rowNum));

      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE:
      case T_DOUBLE_DENSE_COMPONENT:
      case T_DOUBLE_SPARSE_COMPONENT:
        return MFactory.intDoubleMapMatrix(matrixId, -1, new HashMap<>(rowNum));

      case T_INT_SPARSE_LONGKEY:
      case T_INT_DENSE_LONGKEY_COMPONENT:
      case T_INT_SPARSE_LONGKEY_COMPONENT:
        return MFactory.longIntMapMatrix(matrixId, -1, new HashMap<>(rowNum));

      case T_LONG_SPARSE_LONGKEY:
      case T_LONG_DENSE_LONGKEY_COMPONENT:
      case T_LONG_SPARSE_LONGKEY_COMPONENT:
        return MFactory.longLongMapMatrix(matrixId, -1, new HashMap<>(rowNum));

      case T_FLOAT_SPARSE_LONGKEY:
      case T_FLOAT_DENSE_LONGKEY_COMPONENT:
      case T_FLOAT_SPARSE_LONGKEY_COMPONENT:
        return MFactory.longFloatMapMatrix(matrixId, -1, new HashMap<>(rowNum));

      case T_DOUBLE_SPARSE_LONGKEY:
      case T_DOUBLE_DENSE_LONGKEY_COMPONENT:
      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
        return MFactory.longDoubleMapMatrix(matrixId, -1, new HashMap<>(rowNum));

      default:
        throw new UnsupportedOperationException("Unsupport row type " + rowType);
    }
  }


  private void loadIntFloatRows(MapMatrix matrix,
      MatrixPartitionMeta partMeta, MatrixLoadContext loadContext, DataInputStream input)
      throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    IntFloatsCol col = new IntFloatsCol(0, new float[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      format.load(col, input);
      IntFloatVector colVec = VFactory.denseFloatVector(partMeta.getSaveColElemNum());
      for (int j = 0; j < saveElemNum; j++) {
        colVec.set(j, col.colElems[j]);
      }
      matrix.setRow(col.colId, colVec);
    }
  }

  private void loadIntDoubleRows(MapMatrix matrix,
      MatrixPartitionMeta partMeta, MatrixLoadContext loadContext, DataInputStream input)
      throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    IntDoublesCol col = new IntDoublesCol(0, new double[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      format.load(col, input);
      IntDoubleVector colVec = VFactory.denseDoubleVector(partMeta.getSaveColElemNum());
      for (int j = 0; j < saveElemNum; j++) {
        colVec.set(j, col.colElems[j]);
      }
      matrix.setRow(col.colId, colVec);
    }
  }

  private void loadIntIntRows(MapMatrix matrix,
      MatrixPartitionMeta partMeta, MatrixLoadContext loadContext, DataInputStream input)
      throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    IntIntsCol col = new IntIntsCol(0, new int[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      format.load(col, input);
      IntIntVector colVec = VFactory.denseIntVector(partMeta.getSaveColElemNum());
      for (int j = 0; j < saveElemNum; j++) {
        colVec.set(j, col.colElems[j]);
      }
      matrix.setRow(col.colId, colVec);
    }
  }


  private void loadIntLongRows(MapMatrix matrix,
      MatrixPartitionMeta partMeta, MatrixLoadContext loadContext, DataInputStream input)
      throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    IntLongsCol col = new IntLongsCol(0, new long[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      format.load(col, input);
      IntLongVector colVec = VFactory.denseLongVector(partMeta.getSaveColElemNum());
      for (int j = 0; j < saveElemNum; j++) {
        colVec.set(j, col.colElems[j]);
      }
      matrix.setRow(col.colId, colVec);
    }
  }

  private void loadLongFloatRows(MapMatrix matrix,
      MatrixPartitionMeta partMeta, MatrixLoadContext loadContext, DataInputStream input)
      throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    LongFloatsCol col = new LongFloatsCol(0, new float[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      format.load(col, input);
      IntFloatVector colVec = VFactory.denseFloatVector(partMeta.getSaveColElemNum());
      for (int j = 0; j < saveElemNum; j++) {
        colVec.set(j, col.colElems[j]);
      }
      matrix.setRow(col.colId, colVec);
    }
  }

  private void loadLongDoubleRows(MapMatrix matrix,
      MatrixPartitionMeta partMeta, MatrixLoadContext loadContext, DataInputStream input)
      throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    LongDoublesCol col = new LongDoublesCol(0, new double[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      format.load(col, input);
      IntDoubleVector colVec = VFactory.denseDoubleVector(partMeta.getSaveColElemNum());
      for (int j = 0; j < saveElemNum; j++) {
        colVec.set(j, col.colElems[j]);
      }
      matrix.setRow(col.colId, colVec);
    }
  }

  private void loadLongIntRows(MapMatrix matrix,
      MatrixPartitionMeta partMeta, MatrixLoadContext loadContext, DataInputStream input)
      throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    LongIntsCol col = new LongIntsCol(0, new int[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      format.load(col, input);
      IntIntVector colVec = VFactory.denseIntVector(partMeta.getSaveColElemNum());
      for (int j = 0; j < saveElemNum; j++) {
        colVec.set(j, col.colElems[j]);
      }
      matrix.setRow(col.colId, colVec);
    }
  }

  private void loadLongLongRows(MapMatrix matrix,
      MatrixPartitionMeta partMeta, MatrixLoadContext loadContext, DataInputStream input)
      throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    LongLongsCol col = new LongLongsCol(0, new long[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      format.load(col, input);
      IntLongVector colVec = VFactory.denseLongVector(partMeta.getSaveColElemNum());
      for (int j = 0; j < saveElemNum; j++) {
        colVec.set(j, col.colElems[j]);
      }
      matrix.setRow(col.colId, colVec);
    }
  }

}
