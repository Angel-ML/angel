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

package com.tencent.angel.model.output.format;

import com.tencent.angel.ml.math2.MFactory;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.matrix.MapMatrix;
import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.ml.math2.matrix.RowBasedMatrix;
import com.tencent.angel.ml.math2.storage.*;
import com.tencent.angel.ml.math2.vector.*;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.model.MatrixLoadContext;
import com.tencent.angel.model.PSMatrixLoadContext;
import com.tencent.angel.model.PSMatrixSaveContext;
import com.tencent.angel.ps.storage.matrix.PartitionSource;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import com.tencent.angel.ps.storage.vector.*;
import com.tencent.angel.utils.Sort;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2FloatMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * Column first format for matrix
 */
public abstract class ColumnFormat extends MatrixFormatImpl {

  private volatile RowType rowType;

  public ColumnFormat(Configuration conf) {
    super(conf);
  }

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for int key float value matrix
   * @param output output stream
   */
  public abstract void save(IntFloatsCol col, DataOutputStream output) throws IOException;

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for int key double value matrix
   * @param output output stream
   */
  public abstract void save(IntDoublesCol col, DataOutputStream output) throws IOException;

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for int key int value matrix
   * @param output output stream
   */
  public abstract void save(IntIntsCol col, DataOutputStream output) throws IOException;

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for int key long value matrix
   * @param output output stream
   */
  public abstract void save(IntLongsCol col, DataOutputStream output) throws IOException;

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for long key float value matrix
   * @param output output stream
   */
  public abstract void save(LongFloatsCol col, DataOutputStream output) throws IOException;

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for long key float double matrix
   * @param output output stream
   */
  public abstract void save(LongDoublesCol col, DataOutputStream output) throws IOException;

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for long key int value matrix
   * @param output output stream
   */
  public abstract void save(LongIntsCol col, DataOutputStream output) throws IOException;

  /**
   * Write a matrix column to output stream
   *
   * @param col matrix column for long key long value matrix
   * @param output output stream
   */
  public abstract void save(LongLongsCol col, DataOutputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for int key float value matrix
   * @param output input stream
   */
  public abstract void load(IntFloatsCol col, DataInputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for int key double value matrix
   * @param output input stream
   */
  public abstract void load(IntDoublesCol col, DataInputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for int key int value matrix
   * @param output input stream
   */
  public abstract void load(IntIntsCol col, DataInputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for int key long value matrix
   * @param output input stream
   */
  public abstract void load(IntLongsCol col, DataInputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for long key float value matrix
   * @param output input stream
   */
  public abstract void load(LongFloatsCol col, DataInputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for long key float value matrix
   * @param output input stream
   */
  public abstract void load(LongDoublesCol col, DataInputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for long key int value matrix
   * @param output input stream
   */
  public abstract void load(LongIntsCol col, DataInputStream output) throws IOException;

  /**
   * Read a matrix column to input stream
   *
   * @param col matrix column for long key long value matrix
   * @param output input stream
   */
  public abstract void load(LongLongsCol col, DataInputStream output) throws IOException;


  @Override
  public void save(ServerPartition part, MatrixPartitionMeta partMeta,
      PSMatrixSaveContext saveContext, DataOutputStream output) throws IOException {
    List<Integer> rowIds = saveContext.getRowIndexes();
    PartitionSource rows = part.getRows();
    if (rowIds == null || rowIds.isEmpty()) {
      Iterator<Map.Entry<Integer, ServerRow>> iter = part.getRows().iterator();
      rowIds = new ArrayList<>();
      while (iter.hasNext()) {
        rowIds.add(iter.next().getKey());
      }
    } else {
      rowIds = filter(part, rowIds);
    }

    rowIds.sort(new Comparator<Integer>() {
      @Override
      public int compare(Integer id1, Integer id2) {
        return id1 - id2;
      }
    });
    int size = rowIds.size();
    ServerRow[] rowList = new ServerRow[size];
    for (int i = 0; i < size; i++) {
      rowList[i] = rows.getRow(rowIds.get(i));
      RowPartitionMeta rowMeta = new RowPartitionMeta(rowIds.get(i), -1, rowList[i].size());
      partMeta.setRowMeta(rowMeta);
    }

    ServerRow row0 = rowList[0];
    partMeta.setSaveColNum(row0.size());
    partMeta.setSaveColElemNum(rowList.length);

    if (row0 instanceof ServerIntFloatRow) {
      saveIntFloatRows(part, rowList, partMeta, saveContext, output);
    } else if (row0 instanceof ServerIntDoubleRow) {
      saveIntDoubleRows(part, rowList, partMeta, saveContext, output);
    } else if (row0 instanceof ServerIntIntRow) {
      saveIntIntRows(part, rowList, partMeta, saveContext, output);
    } else if (row0 instanceof ServerIntLongRow) {
      saveIntLongRows(part, rowList, partMeta, saveContext, output);
    } else if (row0 instanceof ServerLongFloatRow) {
      saveLongFloatRows(part, rowList, partMeta, saveContext, output);
    } else if (row0 instanceof ServerLongDoubleRow) {
      saveLongDoubleRows(part, rowList, partMeta, saveContext, output);
    } else if (row0 instanceof ServerLongIntRow) {
      saveLongIntRows(part, rowList, partMeta, saveContext, output);
    } else if (row0 instanceof ServerLongLongRow) {
      saveLongLongRows(part, rowList, partMeta, saveContext, output);
    } else {
      throw new UnsupportedOperationException(
          "Unknown server row type " + row0.getClass().getName());
    }
  }

  @Override
  public void load(ServerPartition part, MatrixPartitionMeta partMeta,
      PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {
    Map<Integer, RowPartitionMeta> rowMetas = partMeta.getRowMetas();
    int[] rowIds = new int[rowMetas.size()];
    ServerRow[] rows = new ServerRow[rowMetas.size()];
    int i = 0;
    for (int rowId : rowMetas.keySet()) {
      rowIds[i] = rowId;
      rows[i] = part.getRow(rowId);
      i++;
    }

    ServerRow row0 = part.getRow(rowIds[0]);
    if (row0 instanceof ServerIntFloatRow) {
      loadIntFloatRows(part, rows, partMeta, loadContext, input);
    } else if (row0 instanceof ServerIntDoubleRow) {
      loadIntDoubleRows(part, rows, partMeta, loadContext, input);
    } else if (row0 instanceof ServerIntIntRow) {
      loadIntIntRows(part, rows, partMeta, loadContext, input);
    } else if (row0 instanceof ServerIntLongRow) {
      loadIntLongRows(part, rows, partMeta, loadContext, input);
    } else if (row0 instanceof ServerLongFloatRow) {
      loadLongFloatRows(part, rows, partMeta, loadContext, input);
    } else if (row0 instanceof ServerLongDoubleRow) {
      loadLongDoubleRows(part, rows, partMeta, loadContext, input);
    } else if (row0 instanceof ServerLongIntRow) {
      loadLongIntRows(part, rows, partMeta, loadContext, input);
    } else if (row0 instanceof ServerLongLongRow) {
      loadLongLongRows(part, rows, partMeta, loadContext, input);
    } else {
      throw new UnsupportedOperationException(
          "Unknown server row type " + row0.getClass().getName());
    }
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


  private void saveIntFloatRows(ServerPartition part, ServerRow[] rows,
      MatrixPartitionMeta partMeta, PSMatrixSaveContext saveContext, DataOutputStream output)
      throws IOException {
    Vector vec = rows[0].getSplit();
    //int size = rows.length;
    int indexOffset = (int) part.getPartitionKey().getStartCol();
    IntFloatVectorStorage storage = ((IntFloatVector) vec).getStorage();

    IntFloatsCol col = new IntFloatsCol(0, new float[rows.length]);

    int startCol = (int) rows[0].getStartCol();
    int endCol = (int) rows[0].getEndCol();
    if (storage.isDense()) {
      for (int i = startCol; i < endCol; i++) {
        col.colId = i;
        for (int j = 0; j < rows.length; j++) {
          col.colElems[j] = ((ServerIntFloatRow) (rows[j])).get(col.colId);
        }
        save(col, output);
      }
    } else {
      if (saveContext.sortFirst()) {
        int[] indices = storage.getIndices();
        Sort.quickSort(indices, 0, indices.length - 1);

        for (int i = 0; i < indices.length; i++) {
          col.colId = indices[i] + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerIntFloatRow) (rows[j])).get(col.colId);
          }
          save(col, output);
        }
      } else {
        ObjectIterator<Int2FloatMap.Entry> iter = storage.entryIterator();
        while (iter.hasNext()) {
          col.colId = iter.next().getIntKey() + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerIntFloatRow) (rows[j])).get(col.colId);
          }
          save(col, output);
        }
      }
    }
  }


  private void saveIntDoubleRows(ServerPartition part, ServerRow[] rows,
      MatrixPartitionMeta partMeta, PSMatrixSaveContext saveContext, DataOutputStream output)
      throws IOException {
    Vector vec = rows[0].getSplit();
    //int size = rows.length;
    int indexOffset = (int) part.getPartitionKey().getStartCol();
    IntDoubleVectorStorage storage = ((IntDoubleVector) vec).getStorage();

    IntDoublesCol col = new IntDoublesCol(0, new double[rows.length]);

    int startCol = (int) rows[0].getStartCol();
    int endCol = (int) rows[0].getEndCol();
    if (storage.isDense()) {
      for (int i = startCol; i < endCol; i++) {
        col.colId = i;
        for (int j = 0; j < rows.length; j++) {
          col.colElems[j] = ((ServerIntDoubleRow) (rows[j])).get(col.colId);
        }
        save(col, output);
      }
    } else {
      if (saveContext.sortFirst()) {
        int[] indices = storage.getIndices();
        Sort.quickSort(indices, 0, indices.length - 1);

        for (int i = 0; i < indices.length; i++) {
          col.colId = indices[i] + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerIntDoubleRow) (rows[j])).get(col.colId);
          }
          save(col, output);
        }
      } else {
        ObjectIterator<Int2DoubleMap.Entry> iter = storage.entryIterator();
        while (iter.hasNext()) {
          col.colId = iter.next().getIntKey() + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerIntDoubleRow) (rows[j])).get(col.colId);
          }
          save(col, output);
        }
      }
    }
  }

  private void saveIntIntRows(ServerPartition part, ServerRow[] rows, MatrixPartitionMeta partMeta,
      PSMatrixSaveContext saveContext, DataOutputStream output) throws IOException {
    Vector vec = rows[0].getSplit();
    //int size = rows.size();
    int indexOffset = (int) part.getPartitionKey().getStartCol();
    IntIntVectorStorage storage = ((IntIntVector) vec).getStorage();

    IntIntsCol col = new IntIntsCol(0, new int[rows.length]);

    int startCol = (int) rows[0].getStartCol();
    int endCol = (int) rows[0].getEndCol();
    if (storage.isDense()) {
      for (int i = startCol; i < endCol; i++) {
        col.colId = i;
        for (int j = 0; j < rows.length; j++) {
          col.colElems[j] = ((ServerIntIntRow) (rows[j])).get(col.colId);
        }
        save(col, output);
      }
    } else {
      if (saveContext.sortFirst()) {
        int[] indices = storage.getIndices();
        Sort.quickSort(indices, 0, indices.length - 1);

        for (int i = 0; i < indices.length; i++) {
          col.colId = indices[i] + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerIntIntRow) (rows[j])).get(col.colId);
          }
          save(col, output);
        }
      } else {
        ObjectIterator<Int2IntMap.Entry> iter = storage.entryIterator();
        while (iter.hasNext()) {
          col.colId = iter.next().getIntKey() + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerIntIntRow) (rows[j])).get(col.colId);
          }
          save(col, output);
        }
      }
    }
  }

  private void saveIntLongRows(ServerPartition part, ServerRow[] rows, MatrixPartitionMeta partMeta,
      PSMatrixSaveContext saveContext, DataOutputStream output) throws IOException {
    Vector vec = rows[0].getSplit();
    //int size = rows.size();
    int indexOffset = (int) part.getPartitionKey().getStartCol();
    IntLongVectorStorage storage = ((IntLongVector) vec).getStorage();

    IntLongsCol col = new IntLongsCol(0, new long[rows.length]);

    int startCol = (int) rows[0].getStartCol();
    int endCol = (int) rows[0].getEndCol();
    if (storage.isDense()) {
      for (int i = startCol; i < endCol; i++) {
        col.colId = i;
        for (int j = 0; j < rows.length; j++) {
          col.colElems[j] = ((ServerIntLongRow) (rows[j])).get(col.colId);
        }
        save(col, output);
      }
    } else {
      if (saveContext.sortFirst()) {
        int[] indices = storage.getIndices();
        Sort.quickSort(indices, 0, indices.length - 1);

        for (int i = 0; i < indices.length; i++) {
          col.colId = indices[i] + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerIntLongRow) (rows[j])).get(col.colId);
          }
          save(col, output);
        }
      } else {
        ObjectIterator<Int2LongMap.Entry> iter = storage.entryIterator();
        while (iter.hasNext()) {
          col.colId = iter.next().getIntKey() + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerIntLongRow) (rows[j])).get(col.colId);
          }
          save(col, output);
        }
      }
    }
  }

  private void saveLongFloatRows(ServerPartition part, ServerRow[] rows,
      MatrixPartitionMeta partMeta, PSMatrixSaveContext saveContext, DataOutputStream output)
      throws IOException {
    Vector vec = rows[0].getSplit();
    //int size = rows.size();
    long indexOffset = part.getPartitionKey().getStartCol();

    LongFloatsCol col = new LongFloatsCol(0, new float[rows.length]);
    if (vec instanceof IntFloatVector) {
      IntFloatVectorStorage storage = ((IntFloatVector) vec).getStorage();

      long startCol = rows[0].getStartCol();
      long endCol = rows[0].getEndCol();
      if (storage.isDense()) {
        for (long i = startCol; i < endCol; i++) {
          col.colId = i;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerLongFloatRow) (rows[j])).get(col.colId);
          }
          save(col, output);
        }
      } else {
        if (saveContext.sortFirst()) {
          int[] indices = storage.getIndices();
          Sort.quickSort(indices, 0, indices.length - 1);

          for (int i = 0; i < indices.length; i++) {
            col.colId = indices[i] + indexOffset;
            for (int j = 0; j < rows.length; j++) {
              col.colElems[j] = ((ServerLongFloatRow) (rows[j])).get(col.colId);
            }
            save(col, output);
          }
        } else {
          ObjectIterator<Int2FloatMap.Entry> iter = storage.entryIterator();
          while (iter.hasNext()) {
            col.colId = iter.next().getIntKey() + indexOffset;
            for (int j = 0; j < rows.length; j++) {
              col.colElems[j] = ((ServerLongFloatRow) (rows[j])).get(col.colId);
            }
            save(col, output);
          }
        }
      }
    } else {
      LongFloatVectorStorage storage = ((LongFloatVector) vec).getStorage();
      if (saveContext.sortFirst()) {
        long[] indices = storage.getIndices();
        Sort.quickSort(indices, 0, indices.length - 1);
        for (int i = 0; i < indices.length; i++) {
          col.colId = indices[i] + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerLongFloatRow) (rows[j])).get(col.colId);
          }
          save(col, output);
        }
      } else {
        ObjectIterator<Long2FloatMap.Entry> iter = storage.entryIterator();
        while (iter.hasNext()) {
          col.colId = iter.next().getLongKey() + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerLongFloatRow) (rows[j])).get(col.colId);
          }
          save(col, output);
        }
      }
    }
  }

  private void saveLongDoubleRows(ServerPartition part, ServerRow[] rows,
      MatrixPartitionMeta partMeta, PSMatrixSaveContext saveContext, DataOutputStream output)
      throws IOException {
    Vector vec = rows[0].getSplit();
    //int size = rows.size();
    long indexOffset = part.getPartitionKey().getStartCol();

    LongDoublesCol col = new LongDoublesCol(0, new double[rows.length]);
    if (vec instanceof IntDoubleVector) {
      IntDoubleVectorStorage storage = ((IntDoubleVector) vec).getStorage();

      long startCol = rows[0].getStartCol();
      long endCol = rows[0].getEndCol();
      if (storage.isDense()) {
        for (long i = startCol; i < endCol; i++) {
          col.colId = i;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerLongDoubleRow) (rows[j])).get(col.colId);
          }
          save(col, output);
        }
      } else {
        if (saveContext.sortFirst()) {
          int[] indices = storage.getIndices();
          Sort.quickSort(indices, 0, indices.length - 1);

          for (int i = 0; i < indices.length; i++) {
            col.colId = indices[i] + indexOffset;
            for (int j = 0; j < rows.length; j++) {
              col.colElems[j] = ((ServerLongDoubleRow) (rows[j])).get(col.colId);
            }
            save(col, output);
          }
        } else {
          ObjectIterator<Int2DoubleMap.Entry> iter = storage.entryIterator();
          while (iter.hasNext()) {
            col.colId = iter.next().getIntKey() + indexOffset;
            for (int j = 0; j < rows.length; j++) {
              col.colElems[j] = ((ServerLongDoubleRow) (rows[j])).get(col.colId);
            }
            save(col, output);
          }
        }
      }
    } else {
      LongDoubleVectorStorage storage = ((LongDoubleVector) vec).getStorage();
      if (saveContext.sortFirst()) {
        long[] indices = storage.getIndices();
        Sort.quickSort(indices, 0, indices.length - 1);

        for (int i = 0; i < indices.length; i++) {
          col.colId = indices[i] + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerLongDoubleRow) (rows[j])).get(col.colId);
          }
          save(col, output);
        }
      } else {
        ObjectIterator<Long2DoubleMap.Entry> iter = storage.entryIterator();
        while (iter.hasNext()) {
          col.colId = iter.next().getLongKey() + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerLongDoubleRow) (rows[j])).get(col.colId);
          }
          save(col, output);
        }
      }
    }
  }

  private void saveLongIntRows(ServerPartition part, ServerRow[] rows, MatrixPartitionMeta partMeta,
      PSMatrixSaveContext saveContext, DataOutputStream output) throws IOException {
    Vector vec = rows[0].getSplit();
    //int size = rows.size();
    long indexOffset = part.getPartitionKey().getStartCol();

    LongIntsCol col = new LongIntsCol(0, new int[rows.length]);
    if (vec instanceof IntIntVector) {
      IntIntVectorStorage storage = ((IntIntVector) vec).getStorage();
      long startCol = rows[0].getStartCol();
      long endCol = rows[0].getEndCol();
      if (storage.isDense()) {
        for (long i = startCol; i < endCol; i++) {
          col.colId = i;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerLongIntRow) (rows[j])).get(col.colId);
          }
          save(col, output);
        }
      } else {
        if (saveContext.sortFirst()) {
          int[] indices = storage.getIndices();
          Sort.quickSort(indices, 0, indices.length - 1);

          for (int i = 0; i < indices.length; i++) {
            col.colId = indices[i] + indexOffset;
            for (int j = 0; j < rows.length; j++) {
              col.colElems[j] = ((ServerLongIntRow) (rows[j])).get(col.colId);
            }
            save(col, output);
          }
        } else {
          ObjectIterator<Int2IntMap.Entry> iter = storage.entryIterator();
          while (iter.hasNext()) {
            col.colId = iter.next().getIntKey() + indexOffset;
            for (int j = 0; j < rows.length; j++) {
              col.colElems[j] = ((ServerLongIntRow) (rows[j])).get(col.colId);
            }
            save(col, output);
          }
        }
      }
    } else {
      LongIntVectorStorage storage = ((LongIntVector) vec).getStorage();
      if (saveContext.sortFirst()) {
        long[] indices = storage.getIndices();
        Sort.quickSort(indices, 0, indices.length - 1);

        for (int i = 0; i < indices.length; i++) {
          col.colId = indices[i] + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerLongIntRow) (rows[j])).get(col.colId);
          }
          save(col, output);
        }
      } else {
        ObjectIterator<Long2IntMap.Entry> iter = storage.entryIterator();
        while (iter.hasNext()) {
          col.colId = iter.next().getLongKey() + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerLongIntRow) (rows[j])).get(col.colId);
          }
          save(col, output);
        }
      }
    }
  }

  private void saveLongLongRows(ServerPartition part, ServerRow[] rows,
      MatrixPartitionMeta partMeta, PSMatrixSaveContext saveContext, DataOutputStream output)
      throws IOException {
    Vector vec = rows[0].getSplit();
    //int size = rows.size();
    long indexOffset = part.getPartitionKey().getStartCol();

    LongLongsCol col = new LongLongsCol(0, new long[rows.length]);
    if (vec instanceof IntLongVector) {
      IntLongVectorStorage storage = ((IntLongVector) vec).getStorage();
      long startCol = rows[0].getStartCol();
      long endCol = rows[0].getEndCol();
      if (storage.isDense()) {
        for (long i = startCol; i < endCol; i++) {
          col.colId = i;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerLongLongRow) (rows[j])).get(col.colId);
          }
          save(col, output);
        }
      } else {
        if (saveContext.sortFirst()) {
          int[] indices = storage.getIndices();
          Sort.quickSort(indices, 0, indices.length - 1);

          for (int i = 0; i < indices.length; i++) {
            col.colId = indices[i] + indexOffset;
            for (int j = 0; j < rows.length; j++) {
              col.colElems[j] = ((ServerLongLongRow) (rows[j])).get(col.colId);
            }
            save(col, output);
          }
        } else {
          ObjectIterator<Int2LongMap.Entry> iter = storage.entryIterator();
          while (iter.hasNext()) {
            col.colId = iter.next().getIntKey() + indexOffset;
            for (int j = 0; j < rows.length; j++) {
              col.colElems[j] = ((ServerLongLongRow) (rows[j])).get(col.colId);
            }
            save(col, output);
          }
        }
      }
    } else {
      LongLongVectorStorage storage = ((LongLongVector) vec).getStorage();
      if (saveContext.sortFirst()) {
        long[] indices = storage.getIndices();
        Sort.quickSort(indices, 0, indices.length - 1);

        for (int i = 0; i < indices.length; i++) {
          col.colId = indices[i] + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerLongLongRow) (rows[j])).get(col.colId);
          }
          save(col, output);
        }
      } else {
        ObjectIterator<Long2LongMap.Entry> iter = storage.entryIterator();
        while (iter.hasNext()) {
          col.colId = iter.next().getLongKey() + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerLongLongRow) (rows[j])).get(col.colId);
          }
          save(col, output);
        }
      }
    }
  }

  private void loadIntFloatRows(ServerPartition part, ServerRow[] rows,
      MatrixPartitionMeta partMeta, PSMatrixLoadContext loadContext, DataInputStream input)
      throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    IntFloatsCol col = new IntFloatsCol(0, new float[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      load(col, input);
      for (int j = 0; j < saveElemNum; j++) {
        ((ServerIntFloatRow) rows[j]).set(col.colId, col.colElems[j]);
      }
    }
  }

  private void loadIntDoubleRows(ServerPartition part, ServerRow[] rows,
      MatrixPartitionMeta partMeta, PSMatrixLoadContext loadContext, DataInputStream input)
      throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    IntDoublesCol col = new IntDoublesCol(0, new double[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      load(col, input);
      for (int j = 0; j < saveElemNum; j++) {
        ((ServerIntDoubleRow) rows[j]).set(col.colId, col.colElems[j]);
      }
    }
  }

  private void loadIntIntRows(ServerPartition part, ServerRow[] rows, MatrixPartitionMeta partMeta,
      PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    IntIntsCol col = new IntIntsCol(0, new int[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      load(col, input);
      for (int j = 0; j < saveElemNum; j++) {
        ((ServerIntIntRow) rows[j]).set(col.colId, col.colElems[j]);
      }
    }
  }

  private void loadIntLongRows(ServerPartition part, ServerRow[] rows, MatrixPartitionMeta partMeta,
      PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    IntLongsCol col = new IntLongsCol(0, new long[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      load(col, input);
      for (int j = 0; j < saveElemNum; j++) {
        ((ServerIntLongRow) rows[j]).set(col.colId, col.colElems[j]);
      }
    }
  }

  private void loadLongFloatRows(ServerPartition part, ServerRow[] rows,
      MatrixPartitionMeta partMeta, PSMatrixLoadContext loadContext, DataInputStream input)
      throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    LongFloatsCol col = new LongFloatsCol(0, new float[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      load(col, input);
      for (int j = 0; j < saveElemNum; j++) {
        ((ServerLongFloatRow) rows[j]).set(col.colId, col.colElems[j]);
      }
    }
  }

  private void loadLongDoubleRows(ServerPartition part, ServerRow[] rows,
      MatrixPartitionMeta partMeta, PSMatrixLoadContext loadContext, DataInputStream input)
      throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    LongDoublesCol col = new LongDoublesCol(0, new double[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      load(col, input);
      for (int j = 0; j < saveElemNum; j++) {
        ((ServerLongDoubleRow) rows[j]).set(col.colId, col.colElems[j]);
      }
    }
  }

  private void loadLongIntRows(ServerPartition part, ServerRow[] rows, MatrixPartitionMeta partMeta,
      PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    LongIntsCol col = new LongIntsCol(0, new int[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      load(col, input);
      for (int j = 0; j < saveElemNum; j++) {
        ((ServerLongIntRow) rows[j]).set(col.colId, col.colElems[j]);
      }
    }
  }

  private void loadLongLongRows(ServerPartition part, ServerRow[] rows,
      MatrixPartitionMeta partMeta, PSMatrixLoadContext loadContext, DataInputStream input)
      throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    LongLongsCol col = new LongLongsCol(0, new long[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      load(col, input);
      for (int j = 0; j < saveElemNum; j++) {
        ((ServerLongLongRow) rows[j]).set(col.colId, col.colElems[j]);
      }
    }
  }

  private void loadIntFloatRows(MapMatrix matrix,
      MatrixPartitionMeta partMeta, MatrixLoadContext loadContext, DataInputStream input)
      throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    IntFloatsCol col = new IntFloatsCol(0, new float[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      load(col, input);
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
      load(col, input);
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
      load(col, input);
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
      load(col, input);
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
      load(col, input);
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
      load(col, input);
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
      load(col, input);
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
      load(col, input);
      IntLongVector colVec = VFactory.denseLongVector(partMeta.getSaveColElemNum());
      for (int j = 0; j < saveElemNum; j++) {
        colVec.set(j, col.colElems[j]);
      }
      matrix.setRow(col.colId, colVec);
    }
  }
}
