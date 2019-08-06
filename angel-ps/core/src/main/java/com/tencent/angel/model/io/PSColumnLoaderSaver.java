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

import com.tencent.angel.ml.math2.storage.IntDoubleVectorStorage;
import com.tencent.angel.ml.math2.storage.IntFloatVectorStorage;
import com.tencent.angel.ml.math2.storage.IntIntVectorStorage;
import com.tencent.angel.ml.math2.storage.IntLongVectorStorage;
import com.tencent.angel.ml.math2.storage.LongDoubleVectorStorage;
import com.tencent.angel.ml.math2.storage.LongFloatVectorStorage;
import com.tencent.angel.ml.math2.storage.LongIntVectorStorage;
import com.tencent.angel.ml.math2.storage.LongLongVectorStorage;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.IntLongVector;
import com.tencent.angel.ml.math2.vector.LongDoubleVector;
import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.math2.vector.LongIntVector;
import com.tencent.angel.ml.math2.vector.LongLongVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.model.ModelIOUtils;
import com.tencent.angel.model.PSMatrixLoadContext;
import com.tencent.angel.model.PSMatrixSaveContext;
import com.tencent.angel.model.output.element.IntDoublesCol;
import com.tencent.angel.model.output.element.IntFloatsCol;
import com.tencent.angel.model.output.element.IntIntsCol;
import com.tencent.angel.model.output.element.IntLongsCol;
import com.tencent.angel.model.output.element.LongDoublesCol;
import com.tencent.angel.model.output.element.LongFloatsCol;
import com.tencent.angel.model.output.element.LongIntsCol;
import com.tencent.angel.model.output.element.LongLongsCol;
import com.tencent.angel.model.output.format.ColumnFormat;
import com.tencent.angel.model.output.format.MatrixPartitionMeta;
import com.tencent.angel.model.output.format.RowPartitionMeta;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerIntDoubleRow;
import com.tencent.angel.ps.storage.vector.ServerIntFloatRow;
import com.tencent.angel.ps.storage.vector.ServerIntIntRow;
import com.tencent.angel.ps.storage.vector.ServerIntLongRow;
import com.tencent.angel.ps.storage.vector.ServerLongDoubleRow;
import com.tencent.angel.ps.storage.vector.ServerLongFloatRow;
import com.tencent.angel.ps.storage.vector.ServerLongIntRow;
import com.tencent.angel.ps.storage.vector.ServerLongLongRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.ServerRowUtils;
import com.tencent.angel.utils.Sort;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2FloatMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;

/**
 * Loader/Saver for column-base matrix, it can only be used in PS
 */
public class PSColumnLoaderSaver extends PSMatrixLoaderSaverImpl {

  private final ColumnFormat format;

  public PSColumnLoaderSaver(ColumnFormat format, Configuration conf) {
    super(conf);
    this.format = format;
  }

  @Override
  public void save(ServerPartition part, MatrixPartitionMeta partMeta,
      PSMatrixSaveContext saveContext, DataOutputStream output) throws IOException {
    RowBasedPartition rowPart = (RowBasedPartition) part;
    List<Integer> rowIds = saveContext.getRowIndexes();

    if (rowIds == null || rowIds.isEmpty()) {
      Iterator<Entry<Integer, ServerRow>> iter = rowPart.getRowsStorage().iterator();
      rowIds = new ArrayList<>();
      while (iter.hasNext()) {
        rowIds.add(iter.next().getKey());
      }
    } else {
      rowIds = ModelIOUtils.filter(rowPart, rowIds);
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
      rowList[i] = rowPart.getRow(rowIds.get(i));
      RowPartitionMeta rowMeta = new RowPartitionMeta(rowIds.get(i), -1, rowList[i].size());
      partMeta.setRowMeta(rowMeta);
    }

    ServerRow row0 = rowList[0];
    partMeta.setSaveColNum(row0.size());
    partMeta.setSaveColElemNum(rowList.length);

    if (row0 instanceof ServerIntFloatRow) {
      saveIntFloatRows(rowPart, rowList, partMeta, saveContext, output);
    } else if (row0 instanceof ServerIntDoubleRow) {
      saveIntDoubleRows(rowPart, rowList, partMeta, saveContext, output);
    } else if (row0 instanceof ServerIntIntRow) {
      saveIntIntRows(rowPart, rowList, partMeta, saveContext, output);
    } else if (row0 instanceof ServerIntLongRow) {
      saveIntLongRows(rowPart, rowList, partMeta, saveContext, output);
    } else if (row0 instanceof ServerLongFloatRow) {
      saveLongFloatRows(rowPart, rowList, partMeta, saveContext, output);
    } else if (row0 instanceof ServerLongDoubleRow) {
      saveLongDoubleRows(rowPart, rowList, partMeta, saveContext, output);
    } else if (row0 instanceof ServerLongIntRow) {
      saveLongIntRows(rowPart, rowList, partMeta, saveContext, output);
    } else if (row0 instanceof ServerLongLongRow) {
      saveLongLongRows(rowPart, rowList, partMeta, saveContext, output);
    } else {
      throw new UnsupportedOperationException(
          "Unknown server row type " + row0.getClass().getName());
    }
  }

  @Override
  public void load(ServerPartition part, MatrixPartitionMeta partMeta,
      PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {
    RowBasedPartition rowPart = (RowBasedPartition) part;
    Map<Integer, RowPartitionMeta> rowMetas = partMeta.getRowMetas();
    int[] rowIds = new int[rowMetas.size()];
    ServerRow[] rows = new ServerRow[rowMetas.size()];
    int i = 0;
    for (int rowId : rowMetas.keySet()) {
      rowIds[i] = rowId;
      rows[i] = rowPart.getRow(rowId);
      i++;
    }

    ServerRow row0 = rowPart.getRow(rowIds[0]);
    if (row0 instanceof ServerIntFloatRow) {
      loadIntFloatRows(rowPart, rows, partMeta, loadContext, input);
    } else if (row0 instanceof ServerIntDoubleRow) {
      loadIntDoubleRows(rowPart, rows, partMeta, loadContext, input);
    } else if (row0 instanceof ServerIntIntRow) {
      loadIntIntRows(rowPart, rows, partMeta, loadContext, input);
    } else if (row0 instanceof ServerIntLongRow) {
      loadIntLongRows(rowPart, rows, partMeta, loadContext, input);
    } else if (row0 instanceof ServerLongFloatRow) {
      loadLongFloatRows(rowPart, rows, partMeta, loadContext, input);
    } else if (row0 instanceof ServerLongDoubleRow) {
      loadLongDoubleRows(rowPart, rows, partMeta, loadContext, input);
    } else if (row0 instanceof ServerLongIntRow) {
      loadLongIntRows(rowPart, rows, partMeta, loadContext, input);
    } else if (row0 instanceof ServerLongLongRow) {
      loadLongLongRows(rowPart, rows, partMeta, loadContext, input);
    } else {
      throw new UnsupportedOperationException(
          "Unknown server row type " + row0.getClass().getName());
    }
  }


  private void saveIntFloatRows(RowBasedPartition part, ServerRow[] rows,
      MatrixPartitionMeta partMeta, PSMatrixSaveContext saveContext, DataOutputStream output)
      throws IOException {
    Vector vec = ServerRowUtils.getVector(rows[0]);
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
        format.save(col, output);
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
          format.save(col, output);
        }
      } else {
        ObjectIterator<Int2FloatMap.Entry> iter = storage.entryIterator();
        while (iter.hasNext()) {
          col.colId = iter.next().getIntKey() + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerIntFloatRow) (rows[j])).get(col.colId);
          }
          format.save(col, output);
        }
      }
    }
  }


  private void saveIntDoubleRows(RowBasedPartition part, ServerRow[] rows,
      MatrixPartitionMeta partMeta, PSMatrixSaveContext saveContext, DataOutputStream output)
      throws IOException {
    Vector vec = ServerRowUtils.getVector(rows[0]);
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
        format.save(col, output);
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
          format.save(col, output);
        }
      } else {
        ObjectIterator<Int2DoubleMap.Entry> iter = storage.entryIterator();
        while (iter.hasNext()) {
          col.colId = iter.next().getIntKey() + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerIntDoubleRow) (rows[j])).get(col.colId);
          }
          format.save(col, output);
        }
      }
    }
  }

  private void saveIntIntRows(RowBasedPartition part, ServerRow[] rows, MatrixPartitionMeta partMeta,
      PSMatrixSaveContext saveContext, DataOutputStream output) throws IOException {
    Vector vec = ServerRowUtils.getVector(rows[0]);
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
        format.save(col, output);
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
          format.save(col, output);
        }
      } else {
        ObjectIterator<Int2IntMap.Entry> iter = storage.entryIterator();
        while (iter.hasNext()) {
          col.colId = iter.next().getIntKey() + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerIntIntRow) (rows[j])).get(col.colId);
          }
          format.save(col, output);
        }
      }
    }
  }

  private void saveIntLongRows(RowBasedPartition part, ServerRow[] rows, MatrixPartitionMeta partMeta,
      PSMatrixSaveContext saveContext, DataOutputStream output) throws IOException {
    Vector vec = ServerRowUtils.getVector(rows[0]);
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
        format.save(col, output);
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
          format.save(col, output);
        }
      } else {
        ObjectIterator<Int2LongMap.Entry> iter = storage.entryIterator();
        while (iter.hasNext()) {
          col.colId = iter.next().getIntKey() + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerIntLongRow) (rows[j])).get(col.colId);
          }
          format.save(col, output);
        }
      }
    }
  }

  private void saveLongFloatRows(RowBasedPartition part, ServerRow[] rows,
      MatrixPartitionMeta partMeta, PSMatrixSaveContext saveContext, DataOutputStream output)
      throws IOException {
    Vector vec = ServerRowUtils.getVector(rows[0]);
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
          format.save(col, output);
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
            format.save(col, output);
          }
        } else {
          ObjectIterator<Int2FloatMap.Entry> iter = storage.entryIterator();
          while (iter.hasNext()) {
            col.colId = iter.next().getIntKey() + indexOffset;
            for (int j = 0; j < rows.length; j++) {
              col.colElems[j] = ((ServerLongFloatRow) (rows[j])).get(col.colId);
            }
            format.save(col, output);
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
          format.save(col, output);
        }
      } else {
        ObjectIterator<Long2FloatMap.Entry> iter = storage.entryIterator();
        while (iter.hasNext()) {
          col.colId = iter.next().getLongKey() + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerLongFloatRow) (rows[j])).get(col.colId);
          }
          format.save(col, output);
        }
      }
    }
  }

  private void saveLongDoubleRows(RowBasedPartition part, ServerRow[] rows,
      MatrixPartitionMeta partMeta, PSMatrixSaveContext saveContext, DataOutputStream output)
      throws IOException {
    Vector vec = ServerRowUtils.getVector(rows[0]);
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
          format.save(col, output);
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
            format.save(col, output);
          }
        } else {
          ObjectIterator<Int2DoubleMap.Entry> iter = storage.entryIterator();
          while (iter.hasNext()) {
            col.colId = iter.next().getIntKey() + indexOffset;
            for (int j = 0; j < rows.length; j++) {
              col.colElems[j] = ((ServerLongDoubleRow) (rows[j])).get(col.colId);
            }
            format.save(col, output);
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
          format.save(col, output);
        }
      } else {
        ObjectIterator<Long2DoubleMap.Entry> iter = storage.entryIterator();
        while (iter.hasNext()) {
          col.colId = iter.next().getLongKey() + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerLongDoubleRow) (rows[j])).get(col.colId);
          }
          format.save(col, output);
        }
      }
    }
  }

  private void saveLongIntRows(RowBasedPartition part, ServerRow[] rows, MatrixPartitionMeta partMeta,
      PSMatrixSaveContext saveContext, DataOutputStream output) throws IOException {
    Vector vec = ServerRowUtils.getVector(rows[0]);
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
          format.save(col, output);
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
            format.save(col, output);
          }
        } else {
          ObjectIterator<Int2IntMap.Entry> iter = storage.entryIterator();
          while (iter.hasNext()) {
            col.colId = iter.next().getIntKey() + indexOffset;
            for (int j = 0; j < rows.length; j++) {
              col.colElems[j] = ((ServerLongIntRow) (rows[j])).get(col.colId);
            }
            format.save(col, output);
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
          format.save(col, output);
        }
      } else {
        ObjectIterator<Long2IntMap.Entry> iter = storage.entryIterator();
        while (iter.hasNext()) {
          col.colId = iter.next().getLongKey() + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerLongIntRow) (rows[j])).get(col.colId);
          }
          format.save(col, output);
        }
      }
    }
  }

  private void saveLongLongRows(RowBasedPartition part, ServerRow[] rows,
      MatrixPartitionMeta partMeta, PSMatrixSaveContext saveContext, DataOutputStream output)
      throws IOException {
    Vector vec = ServerRowUtils.getVector(rows[0]);
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
          format.save(col, output);
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
            format.save(col, output);
          }
        } else {
          ObjectIterator<Int2LongMap.Entry> iter = storage.entryIterator();
          while (iter.hasNext()) {
            col.colId = iter.next().getIntKey() + indexOffset;
            for (int j = 0; j < rows.length; j++) {
              col.colElems[j] = ((ServerLongLongRow) (rows[j])).get(col.colId);
            }
            format.save(col, output);
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
          format.save(col, output);
        }
      } else {
        ObjectIterator<Long2LongMap.Entry> iter = storage.entryIterator();
        while (iter.hasNext()) {
          col.colId = iter.next().getLongKey() + indexOffset;
          for (int j = 0; j < rows.length; j++) {
            col.colElems[j] = ((ServerLongLongRow) (rows[j])).get(col.colId);
          }
          format.save(col, output);
        }
      }
    }
  }

  private void loadIntFloatRows(RowBasedPartition part, ServerRow[] rows,
      MatrixPartitionMeta partMeta, PSMatrixLoadContext loadContext, DataInputStream input)
      throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    IntFloatsCol col = new IntFloatsCol(0, new float[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      format.load(col, input);
      for (int j = 0; j < saveElemNum; j++) {
        ((ServerIntFloatRow) rows[j]).set(col.colId, col.colElems[j]);
      }
    }
  }

  private void loadIntDoubleRows(RowBasedPartition part, ServerRow[] rows,
      MatrixPartitionMeta partMeta, PSMatrixLoadContext loadContext, DataInputStream input)
      throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    IntDoublesCol col = new IntDoublesCol(0, new double[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      format.load(col, input);
      for (int j = 0; j < saveElemNum; j++) {
        ((ServerIntDoubleRow) rows[j]).set(col.colId, col.colElems[j]);
      }
    }
  }

  private void loadIntIntRows(RowBasedPartition part, ServerRow[] rows, MatrixPartitionMeta partMeta,
      PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    IntIntsCol col = new IntIntsCol(0, new int[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      format.load(col, input);
      for (int j = 0; j < saveElemNum; j++) {
        ((ServerIntIntRow) rows[j]).set(col.colId, col.colElems[j]);
      }
    }
  }

  private void loadIntLongRows(RowBasedPartition part, ServerRow[] rows, MatrixPartitionMeta partMeta,
      PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    IntLongsCol col = new IntLongsCol(0, new long[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      format.load(col, input);
      for (int j = 0; j < saveElemNum; j++) {
        ((ServerIntLongRow) rows[j]).set(col.colId, col.colElems[j]);
      }
    }
  }

  private void loadLongFloatRows(RowBasedPartition part, ServerRow[] rows,
      MatrixPartitionMeta partMeta, PSMatrixLoadContext loadContext, DataInputStream input)
      throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    LongFloatsCol col = new LongFloatsCol(0, new float[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      format.load(col, input);
      for (int j = 0; j < saveElemNum; j++) {
        ((ServerLongFloatRow) rows[j]).set(col.colId, col.colElems[j]);
      }
    }
  }

  private void loadLongDoubleRows(RowBasedPartition part, ServerRow[] rows,
      MatrixPartitionMeta partMeta, PSMatrixLoadContext loadContext, DataInputStream input)
      throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    LongDoublesCol col = new LongDoublesCol(0, new double[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      format.load(col, input);
      for (int j = 0; j < saveElemNum; j++) {
        ((ServerLongDoubleRow) rows[j]).set(col.colId, col.colElems[j]);
      }
    }
  }

  private void loadLongIntRows(RowBasedPartition part, ServerRow[] rows, MatrixPartitionMeta partMeta,
      PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    LongIntsCol col = new LongIntsCol(0, new int[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      format.load(col, input);
      for (int j = 0; j < saveElemNum; j++) {
        ((ServerLongIntRow) rows[j]).set(col.colId, col.colElems[j]);
      }
    }
  }

  private void loadLongLongRows(RowBasedPartition part, ServerRow[] rows,
      MatrixPartitionMeta partMeta, PSMatrixLoadContext loadContext, DataInputStream input)
      throws IOException {
    int saveColNum = partMeta.getSaveColNum();
    int saveElemNum = partMeta.getSaveColElemNum();
    LongLongsCol col = new LongLongsCol(0, new long[saveElemNum]);

    for (int i = 0; i < saveColNum; i++) {
      format.load(col, input);
      for (int j = 0; j < saveElemNum; j++) {
        ((ServerLongLongRow) rows[j]).set(col.colId, col.colElems[j]);
      }
    }
  }
}
