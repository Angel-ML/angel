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

import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.ml.math2.vector.*;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.model.MatrixLoadContext;
import com.tencent.angel.model.PSMatrixLoadContext;
import com.tencent.angel.model.PSMatrixSaveContext;
import com.tencent.angel.ps.storage.matrix.PartitionSource;
import com.tencent.angel.ps.storage.matrix.PartitionState;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import com.tencent.angel.ps.storage.vector.*;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * Base class for row first output format
 */
public abstract class RowFormat extends MatrixFormatImpl {
  private final static Log LOG = LogFactory.getLog(RowFormat.class);

  public RowFormat(Configuration conf) {
    super(conf);
  }

  /**
   * Save a (int, float) element
   *
   * @param element a (int, float) element
   * @param out     output stream
   * @throws IOException
   */
  public abstract void save(IntFloatElement element, DataOutputStream out) throws IOException;

  /**
   * Save a (int, double) element
   *
   * @param element a (int, double) element
   * @param out     output stream
   * @throws IOException
   */
  public abstract void save(IntDoubleElement element, DataOutputStream out) throws IOException;

  /**
   * Save a (int, int) element
   *
   * @param element a (int, int) element
   * @param out     output stream
   * @throws IOException
   */
  public abstract void save(IntIntElement element, DataOutputStream out) throws IOException;

  /**
   * Save a (int, long) element
   *
   * @param element a (int, long) element
   * @param out     output stream
   * @throws IOException
   */
  public abstract void save(IntLongElement element, DataOutputStream out) throws IOException;

  /**
   * Save a (long, float) element
   *
   * @param element a (long, float) element
   * @param out     output stream
   * @throws IOException
   */
  public abstract void save(LongFloatElement element, DataOutputStream out) throws IOException;

  /**
   * Save a (long, double) element
   *
   * @param element a (long, double) element
   * @param out     output stream
   * @throws IOException
   */
  public abstract void save(LongDoubleElement element, DataOutputStream out) throws IOException;

  /**
   * Save a (long, int) element
   *
   * @param element a (long, int) element
   * @param out     output stream
   * @throws IOException
   */
  public abstract void save(LongIntElement element, DataOutputStream out) throws IOException;

  /**
   * Save a (long, long) element
   *
   * @param element a (long, long) element
   * @param out     output stream
   * @throws IOException
   */
  public abstract void save(LongLongElement element, DataOutputStream out) throws IOException;

  /**
   * Load a (int, float) element
   *
   * @param element a (int, float) element
   * @param in      input stream
   * @throws IOException
   */
  public abstract void load(IntFloatElement element, DataInputStream in) throws IOException;

  /**
   * Load a (int, double) element
   *
   * @param element a (int, double) element
   * @param in      input stream
   * @throws IOException
   */
  public abstract void load(IntDoubleElement element, DataInputStream in) throws IOException;

  /**
   * Load a (int, int) element
   *
   * @param element a (int, int) element
   * @param in      input stream
   * @throws IOException
   */
  public abstract void load(IntIntElement element, DataInputStream in) throws IOException;

  /**
   * Load a (int, long) element
   *
   * @param element a (int, long) element
   * @param in      input stream
   * @throws IOException
   */
  public abstract void load(IntLongElement element, DataInputStream in) throws IOException;

  /**
   * Load a (long, float) element
   *
   * @param element a (long, float) element
   * @param in      input stream
   * @throws IOException
   */
  public abstract void load(LongFloatElement element, DataInputStream in) throws IOException;

  /**
   * Load a (long, double) element
   *
   * @param element a (long, double) element
   * @param in      input stream
   * @throws IOException
   */
  public abstract void load(LongDoubleElement element, DataInputStream in) throws IOException;

  /**
   * Load a (long, int) element
   *
   * @param element a (long, int) element
   * @param in      input stream
   * @throws IOException
   */
  public abstract void load(LongIntElement element, DataInputStream in) throws IOException;

  /**
   * Load a (long, long) element
   *
   * @param element a (long, long) element
   * @param in      input stream
   * @throws IOException
   */
  public abstract void load(LongLongElement element, DataInputStream in) throws IOException;

  /**
   * Matrix partition data
   *
   * @param part        matrix partition
   * @param partMeta    matrix partition data meta
   * @param saveContext save context
   * @param output      output stream
   * @throws IOException
   */
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

    FSDataOutputStream dataOutputStream =
      new FSDataOutputStream(output, null, partMeta != null ? partMeta.getOffset() : 0);

    partMeta.setSaveRowNum(rowIds.size());
    for (int rowId : rowIds) {
      ServerRow row = rows.getRow(rowId);
      RowPartitionMeta rowMeta = new RowPartitionMeta(rowId, 0, 0);
      if (row != null) {
        rowMeta.setElementNum(row.size());
        rowMeta.setOffset(dataOutputStream.getPos());
        save(rows.getRow(rowId), saveContext, partMeta, output);
      } else {
        rowMeta.setElementNum(0);
        rowMeta.setOffset(dataOutputStream.getPos());
      }
      partMeta.setRowMeta(rowMeta);
    }
  }

  /**
   * Save a row
   *
   * @param row         row data
   * @param saveContext save context
   * @param meta        partition data meta
   * @param out         output stream
   * @throws IOException
   */
  public void save(ServerRow row, PSMatrixSaveContext saveContext, MatrixPartitionMeta meta,
    DataOutputStream out) throws IOException {
    if (saveContext.cloneFirst()) {
      row = row.clone();
    }
    try {
      row.startWrite();
      if (row instanceof ServerIntFloatRow) {
        save((ServerIntFloatRow) row, saveContext, meta, out);
      } else if (row instanceof ServerIntDoubleRow) {
        save((ServerIntDoubleRow) row, saveContext, meta, out);
      } else if (row instanceof ServerIntIntRow) {
        save((ServerIntIntRow) row, saveContext, meta, out);
      } else if (row instanceof ServerIntLongRow) {
        save((ServerIntLongRow) row, saveContext, meta, out);
      } else if (row instanceof ServerLongFloatRow) {
        save((ServerLongFloatRow) row, saveContext, meta, out);
      } else if (row instanceof ServerLongDoubleRow) {
        save((ServerLongDoubleRow) row, saveContext, meta, out);
      } else if (row instanceof ServerLongIntRow) {
        save((ServerLongIntRow) row, saveContext, meta, out);
      } else if (row instanceof ServerLongLongRow) {
        save((ServerLongLongRow) row, saveContext, meta, out);
      } else {
        throw new IOException("Unknown vector type " + row.getRowType());
      }
    } finally {
      row.endWrite();
    }
  }

  /**
   * Load a matrix partition
   *
   * @param part        matrix partition
   * @param partMeta    matrix partition data meta
   * @param loadContext load context
   * @param input       input stream
   * @throws IOException
   */
  public void load(ServerPartition part, MatrixPartitionMeta partMeta,
    PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {
    PartitionSource rows = part.getRows();
    try {
      Map<Integer, RowPartitionMeta> rowMetas = partMeta.getRowMetas();
      for (RowPartitionMeta rowMeta : rowMetas.values()) {
        ServerRow row = rows.getRow(rowMeta.getRowId());
        load(row, partMeta, loadContext, input);
      }
    } finally {
      part.setState(PartitionState.READ_AND_WRITE);
    }
  }

  /**
   * Load a row data
   *
   * @param row         row partition
   * @param meta        partition meta
   * @param loadContext load context
   * @param in          input stream
   * @throws IOException
   */
  public void load(ServerRow row, MatrixPartitionMeta meta, PSMatrixLoadContext loadContext,
    DataInputStream in) throws IOException {
    try {
      row.startWrite();
      if (row instanceof ServerIntFloatRow) {
        load((ServerIntFloatRow) row, loadContext, meta, in);
      } else if (row instanceof ServerIntDoubleRow) {
        load((ServerIntDoubleRow) row, loadContext, meta, in);
      } else if (row instanceof ServerIntIntRow) {
        load((ServerIntIntRow) row, loadContext, meta, in);
      } else if (row instanceof ServerIntLongRow) {
        load((ServerIntLongRow) row, loadContext, meta, in);
      } else if (row instanceof ServerLongFloatRow) {
        load((ServerLongFloatRow) row, loadContext, meta, in);
      } else if (row instanceof ServerLongDoubleRow) {
        load((ServerLongDoubleRow) row, loadContext, meta, in);
      } else if (row instanceof ServerLongIntRow) {
        load((ServerLongIntRow) row, loadContext, meta, in);
      } else if (row instanceof ServerLongLongRow) {
        load((ServerLongLongRow) row, loadContext, meta, in);
      } else {
        throw new IOException("Unknown vector type " + row.getRowType());
      }
    } finally {
      row.endWrite();
    }
  }

  private void save(ServerIntFloatRow row, PSMatrixSaveContext saveContext,
    MatrixPartitionMeta meta, DataOutputStream out) throws IOException {
    int startCol = (int) meta.getStartCol();
    IntFloatVector vector = (IntFloatVector) row.getSplit();
    IntFloatElement element = new IntFloatElement();
    if (vector.isDense()) {
      float[] data = vector.getStorage().getValues();
      for (int i = 0; i < data.length; i++) {
        element.rowId = row.getRowId();
        element.colId = startCol + i;
        element.value = data[i];
        save(element, out);
      }
    } else {
      if (saveContext.sortFirst()) {
        int[] indices = vector.getStorage().getIndices();
        float[] values = vector.getStorage().getValues();
        Sort.quickSort(indices, values, 0, indices.length - 1);
        for (int i = 0; i < indices.length; i++) {
          element.rowId = row.getRowId();
          element.colId = indices[i] + startCol;
          element.value = values[i];
          save(element, out);
        }
      } else {
        ObjectIterator<Int2FloatMap.Entry> iter = vector.getStorage().entryIterator();
        Int2FloatMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          element.rowId = row.getRowId();
          element.colId = entry.getIntKey() + startCol;
          element.value = entry.getFloatValue();
          save(element, out);
        }
      }
    }
  }

  private void save(ServerIntDoubleRow row, PSMatrixSaveContext saveContext,
    MatrixPartitionMeta meta, DataOutputStream out) throws IOException {
    int startCol = (int) meta.getStartCol();
    IntDoubleVector vector = (IntDoubleVector) row.getSplit();
    IntDoubleElement element = new IntDoubleElement();
    if (vector.isDense()) {
      double[] data = vector.getStorage().getValues();
      for (int i = 0; i < data.length; i++) {
        element.rowId = row.getRowId();
        element.colId = startCol + i;
        element.value = data[i];
        save(element, out);
      }
    } else {
      if (saveContext.sortFirst()) {
        int[] indices = vector.getStorage().getIndices();
        double[] values = vector.getStorage().getValues();
        Sort.quickSort(indices, values, 0, indices.length - 1);
        for (int i = 0; i < indices.length; i++) {
          element.rowId = row.getRowId();
          element.colId = indices[i] + startCol;
          element.value = values[i];
          save(element, out);
        }
      } else {
        ObjectIterator<Int2DoubleMap.Entry> iter = vector.getStorage().entryIterator();
        Int2DoubleMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          element.rowId = row.getRowId();
          element.colId = entry.getIntKey() + startCol;
          element.value = entry.getDoubleValue();
          save(element, out);
        }
      }
    }
  }

  private void save(ServerIntIntRow row, PSMatrixSaveContext saveContext, MatrixPartitionMeta meta,
    DataOutputStream out) throws IOException {
    int startCol = (int) meta.getStartCol();
    IntIntVector vector = (IntIntVector) row.getSplit();
    IntIntElement element = new IntIntElement();
    if (vector.isDense()) {
      int[] data = vector.getStorage().getValues();
      for (int i = 0; i < data.length; i++) {
        element.rowId = row.getRowId();
        element.colId = startCol + i;
        element.value = data[i];
        save(element, out);
      }
    } else {
      if (saveContext.sortFirst()) {
        int[] indices = vector.getStorage().getIndices();
        int[] values = vector.getStorage().getValues();
        Sort.quickSort(indices, values, 0, indices.length - 1);
        for (int i = 0; i < indices.length; i++) {
          element.rowId = row.getRowId();
          element.colId = indices[i] + startCol;
          element.value = values[i];
          save(element, out);
        }
      } else {
        ObjectIterator<Int2IntMap.Entry> iter = vector.getStorage().entryIterator();
        Int2IntMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          element.rowId = row.getRowId();
          element.colId = entry.getIntKey() + startCol;
          element.value = entry.getIntValue();
          save(element, out);
        }
      }
    }
  }

  private void save(ServerIntLongRow row, PSMatrixSaveContext saveContext, MatrixPartitionMeta meta,
    DataOutputStream out) throws IOException {
    int startCol = (int) meta.getStartCol();
    IntLongVector vector = (IntLongVector) row.getSplit();
    IntLongElement element = new IntLongElement();
    if (vector.isDense()) {
      long[] data = vector.getStorage().getValues();
      for (int i = 0; i < data.length; i++) {
        element.rowId = row.getRowId();
        element.colId = startCol + i;
        element.value = data[i];
        save(element, out);
      }
    } else {
      if (saveContext.sortFirst()) {
        int[] indices = vector.getStorage().getIndices();
        long[] values = vector.getStorage().getValues();
        Sort.quickSort(indices, values, 0, indices.length - 1);
        for (int i = 0; i < indices.length; i++) {
          element.rowId = row.getRowId();
          element.colId = indices[i] + startCol;
          element.value = values[i];
          save(element, out);
        }
      } else {
        ObjectIterator<Int2LongMap.Entry> iter = vector.getStorage().entryIterator();
        Int2LongMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          element.rowId = row.getRowId();
          element.colId = entry.getIntKey() + startCol;
          element.value = entry.getLongValue();
          save(element, out);
        }
      }
    }
  }

  private void save(ServerLongDoubleRow row, PSMatrixSaveContext saveContext,
    MatrixPartitionMeta meta, DataOutputStream out) throws IOException {
    long startCol = meta.getStartCol();
    LongDoubleElement element = new LongDoubleElement();
    if (row.getSplit() instanceof IntDoubleVector) {
      IntDoubleVector vector = (IntDoubleVector) row.getSplit();
      if (vector.isDense()) {
        double[] data = vector.getStorage().getValues();
        for (int i = 0; i < data.length; i++) {
          element.rowId = row.getRowId();
          element.colId = startCol + i;
          element.value = data[i];
          save(element, out);
        }
      } else {
        if (saveContext.sortFirst()) {
          int[] indices = vector.getStorage().getIndices();
          double[] values = vector.getStorage().getValues();
          Sort.quickSort(indices, values, 0, indices.length - 1);
          for (int i = 0; i < indices.length; i++) {
            element.rowId = row.getRowId();
            element.colId = indices[i] + startCol;
            element.value = values[i];
            save(element, out);
          }
        } else {
          ObjectIterator<Int2DoubleMap.Entry> iter = vector.getStorage().entryIterator();
          Int2DoubleMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            element.rowId = row.getRowId();
            element.colId = entry.getIntKey() + startCol;
            element.value = entry.getDoubleValue();
            save(element, out);
          }
        }
      }
    } else {
      LongDoubleVector vector = (LongDoubleVector) row.getSplit();
      if (saveContext.sortFirst()) {
        long[] indices = vector.getStorage().getIndices();
        double[] values = vector.getStorage().getValues();
        Sort.quickSort(indices, values, 0, indices.length - 1);
        for (int i = 0; i < indices.length; i++) {
          element.rowId = row.getRowId();
          element.colId = indices[i] + startCol;
          element.value = values[i];
          save(element, out);
        }
      } else {
        ObjectIterator<Long2DoubleMap.Entry> iter = vector.getStorage().entryIterator();
        Long2DoubleMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          element.rowId = row.getRowId();
          element.colId = entry.getLongKey() + startCol;
          element.value = entry.getDoubleValue();
          save(element, out);
        }
      }
    }
  }

  private void save(ServerLongFloatRow row, PSMatrixSaveContext saveContext,
    MatrixPartitionMeta meta, DataOutputStream out) throws IOException {
    long startCol = meta.getStartCol();
    LongFloatElement element = new LongFloatElement();
    if (row.getSplit() instanceof IntFloatVector) {
      IntFloatVector vector = (IntFloatVector) row.getSplit();
      if (vector.isDense()) {
        float[] data = vector.getStorage().getValues();
        for (int i = 0; i < data.length; i++) {
          element.rowId = row.getRowId();
          element.colId = startCol + i;
          element.value = data[i];
          save(element, out);
        }
      } else {
        if (saveContext.sortFirst()) {
          int[] indices = vector.getStorage().getIndices();
          float[] values = vector.getStorage().getValues();
          Sort.quickSort(indices, values, 0, indices.length - 1);
          for (int i = 0; i < indices.length; i++) {
            element.rowId = row.getRowId();
            element.colId = indices[i] + startCol;
            element.value = values[i];
            save(element, out);
          }
        } else {
          ObjectIterator<Int2FloatMap.Entry> iter = vector.getStorage().entryIterator();
          Int2FloatMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            element.rowId = row.getRowId();
            element.colId = entry.getIntKey() + startCol;
            element.value = entry.getFloatValue();
            save(element, out);
          }
        }
      }
    } else {
      LongFloatVector vector = (LongFloatVector) row.getSplit();
      if (saveContext.sortFirst()) {
        long[] indices = vector.getStorage().getIndices();
        float[] values = vector.getStorage().getValues();
        Sort.quickSort(indices, values, 0, indices.length - 1);
        for (int i = 0; i < indices.length; i++) {
          element.rowId = row.getRowId();
          element.colId = indices[i] + startCol;
          element.value = values[i];
          save(element, out);
        }
      } else {
        ObjectIterator<Long2FloatMap.Entry> iter = vector.getStorage().entryIterator();
        Long2FloatMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          element.rowId = row.getRowId();
          element.colId = entry.getLongKey() + startCol;
          element.value = entry.getFloatValue();
          save(element, out);
        }
      }
    }
  }


  private void save(ServerLongIntRow row, PSMatrixSaveContext saveContext, MatrixPartitionMeta meta,
    DataOutputStream out) throws IOException {
    long startCol = meta.getStartCol();
    LongIntElement element = new LongIntElement();
    if (row.getSplit() instanceof IntIntVector) {
      IntIntVector vector = (IntIntVector) row.getSplit();
      if (vector.isDense()) {
        int[] data = vector.getStorage().getValues();
        for (int i = 0; i < data.length; i++) {
          element.rowId = row.getRowId();
          element.colId = startCol + i;
          element.value = data[i];
          save(element, out);
        }
      } else {
        if (saveContext.sortFirst()) {
          int[] indices = vector.getStorage().getIndices();
          int[] values = vector.getStorage().getValues();
          Sort.quickSort(indices, values, 0, indices.length - 1);
          for (int i = 0; i < indices.length; i++) {
            element.rowId = row.getRowId();
            element.colId = indices[i] + startCol;
            element.value = values[i];
            save(element, out);
          }
        } else {
          ObjectIterator<Int2IntMap.Entry> iter = vector.getStorage().entryIterator();
          Int2IntMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            element.rowId = row.getRowId();
            element.colId = entry.getIntKey() + startCol;
            element.value = entry.getIntValue();
            save(element, out);
          }
        }
      }
    } else {
      LongIntVector vector = (LongIntVector) row.getSplit();
      if (saveContext.sortFirst()) {
        long[] indices = vector.getStorage().getIndices();
        int[] values = vector.getStorage().getValues();
        Sort.quickSort(indices, values, 0, indices.length - 1);
        for (int i = 0; i < indices.length; i++) {
          element.rowId = row.getRowId();
          element.colId = indices[i] + startCol;
          element.value = values[i];
          save(element, out);
        }
      } else {
        ObjectIterator<Long2IntMap.Entry> iter = vector.getStorage().entryIterator();
        Long2IntMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          element.rowId = row.getRowId();
          element.colId = entry.getLongKey() + startCol;
          element.value = entry.getIntValue();
          save(element, out);
        }
      }
    }
  }

  private void save(ServerLongLongRow row, PSMatrixSaveContext saveContext,
    MatrixPartitionMeta meta, DataOutputStream out) throws IOException {
    long startCol = meta.getStartCol();
    LongLongElement element = new LongLongElement();
    if (row.getSplit() instanceof IntLongVector) {
      IntLongVector vector = (IntLongVector) row.getSplit();
      if (vector.isDense()) {
        long[] data = vector.getStorage().getValues();
        for (int i = 0; i < data.length; i++) {
          element.rowId = row.getRowId();
          element.colId = startCol + i;
          element.value = data[i];
          save(element, out);
        }
      } else {
        if (saveContext.sortFirst()) {
          int[] indices = vector.getStorage().getIndices();
          long[] values = vector.getStorage().getValues();
          Sort.quickSort(indices, values, 0, indices.length - 1);
          for (int i = 0; i < indices.length; i++) {
            element.rowId = row.getRowId();
            element.colId = indices[i] + startCol;
            element.value = values[i];
            save(element, out);
          }
        } else {
          ObjectIterator<Int2LongMap.Entry> iter = vector.getStorage().entryIterator();
          Int2LongMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            element.rowId = row.getRowId();
            element.colId = entry.getIntKey() + startCol;
            element.value = entry.getLongValue();
            save(element, out);
          }
        }
      }
    } else {
      LongLongVector vector = (LongLongVector) row.getSplit();
      if (saveContext.sortFirst()) {
        long[] indices = vector.getStorage().getIndices();
        long[] values = vector.getStorage().getValues();
        Sort.quickSort(indices, values, 0, indices.length - 1);
        for (int i = 0; i < indices.length; i++) {
          element.rowId = row.getRowId();
          element.colId = indices[i] + startCol;
          element.value = values[i];
          save(element, out);
        }
      } else {
        ObjectIterator<Long2LongMap.Entry> iter = vector.getStorage().entryIterator();
        Long2LongMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          element.rowId = row.getRowId();
          element.colId = entry.getLongKey() + startCol;
          element.value = entry.getLongValue();
          save(element, out);
        }
      }
    }
  }

  private void load(ServerIntFloatRow row, PSMatrixLoadContext loadContext,
    MatrixPartitionMeta meta, DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();
    IntFloatElement element = new IntFloatElement();
    for (int i = 0; i < elemNum; i++) {
      element.colId = i + (int)row.getStartCol();
      load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(ServerIntDoubleRow row, PSMatrixLoadContext loadContext,
    MatrixPartitionMeta meta, DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();
    IntDoubleElement element = new IntDoubleElement();
    for (int i = 0; i < elemNum; i++) {
      element.colId = i + (int)row.getStartCol();
      load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(ServerIntIntRow row, PSMatrixLoadContext loadContext, MatrixPartitionMeta meta,
    DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();
    IntIntElement element = new IntIntElement();
    for (int i = 0; i < elemNum; i++) {
      element.colId = i + (int)row.getStartCol();
      load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(ServerIntLongRow row, PSMatrixLoadContext loadContext, MatrixPartitionMeta meta,
    DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();
    IntLongElement element = new IntLongElement();
    for (int i = 0; i < elemNum; i++) {
      element.colId = i + (int)row.getStartCol();
      load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(ServerLongFloatRow row, PSMatrixLoadContext loadContext,
    MatrixPartitionMeta meta, DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();
    LongFloatElement element = new LongFloatElement();
    for (int i = 0; i < elemNum; i++) {
      element.colId = i + row.getStartCol();
      load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(ServerLongDoubleRow row, PSMatrixLoadContext loadContext,
    MatrixPartitionMeta meta, DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();
    LongDoubleElement element = new LongDoubleElement();
    for (int i = 0; i < elemNum; i++) {
      element.colId = i + row.getStartCol();
      load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(ServerLongIntRow row, PSMatrixLoadContext loadContext, MatrixPartitionMeta meta,
    DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();
    LongIntElement element = new LongIntElement();
    for (int i = 0; i < elemNum; i++) {
      element.colId = i + row.getStartCol();
      load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(ServerLongLongRow row, PSMatrixLoadContext loadContext,
    MatrixPartitionMeta meta, DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();
    LongLongElement element = new LongLongElement();
    for (int i = 0; i < elemNum; i++) {
      element.colId = i + row.getStartCol();
      load(element, in);
      row.set(element.colId, element.value);
    }
  }

  public void load(Matrix matrix, MatrixPartitionMeta partMeta, MatrixLoadContext loadContext,
    FSDataInputStream in) throws IOException {
    Map<Integer, RowPartitionMeta> rowMetas = partMeta.getRowMetas();
    for (int rowId : rowMetas.keySet()) {
      Vector row = matrix.getRow(rowId);
      load(row, partMeta, loadContext, in);
    }
  }

  public void load(Vector row, MatrixPartitionMeta meta, MatrixLoadContext loadContext,
    DataInputStream in) throws IOException {
    try {
      if (row instanceof IntFloatVector) {
        load((IntFloatVector) row, loadContext, meta, in);
      } else if (row instanceof IntDoubleVector) {
        load((IntDoubleVector) row, loadContext, meta, in);
      } else if (row instanceof IntIntVector) {
        load((IntIntVector) row, loadContext, meta, in);
      } else if (row instanceof IntLongVector) {
        load((IntLongVector) row, loadContext, meta, in);
      } else if (row instanceof LongFloatVector) {
        load((LongFloatVector) row, loadContext, meta, in);
      } else if (row instanceof LongDoubleVector) {
        load((LongDoubleVector) row, loadContext, meta, in);
      } else if (row instanceof LongIntVector) {
        load((LongIntVector) row, loadContext, meta, in);
      } else if (row instanceof LongLongVector) {
        load((LongLongVector) row, loadContext, meta, in);
      } else {
        throw new IOException("Unknown vector type " + row.getType());
      }
    } finally {
    }
  }

  private void load(IntFloatVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
    DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if(rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }
    int elemNum = rowMeta.getElementNum();
    IntFloatElement element = new IntFloatElement();
    for (int i = 0; i < elemNum; i++) {
      load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(IntDoubleVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
    DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if(rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }
    int elemNum = rowMeta.getElementNum();
    IntDoubleElement element = new IntDoubleElement();
    for (int i = 0; i < elemNum; i++) {
      load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(IntIntVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
    DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if(rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }
    int elemNum = rowMeta.getElementNum();
    IntIntElement element = new IntIntElement();
    for (int i = 0; i < elemNum; i++) {
      load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(IntLongVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
    DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if(rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }
    int elemNum = rowMeta.getElementNum();
    IntLongElement element = new IntLongElement();
    for (int i = 0; i < elemNum; i++) {
      load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(LongFloatVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
    DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if(rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }
    int elemNum = rowMeta.getElementNum();
    LongFloatElement element = new LongFloatElement();
    for (int i = 0; i < elemNum; i++) {
      load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(LongDoubleVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
    DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if(rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }
    int elemNum = rowMeta.getElementNum();
    LongDoubleElement element = new LongDoubleElement();
    for (int i = 0; i < elemNum; i++) {
      load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(LongIntVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
    DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if(rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }
    int elemNum = rowMeta.getElementNum();
    LongIntElement element = new LongIntElement();
    for (int i = 0; i < elemNum; i++) {
      load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(LongLongVector row, MatrixLoadContext loadContext, MatrixPartitionMeta meta,
    DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    if(rowMeta == null) {
      LOG.warn("can not find meta data for row " + row.getRowId() + ", just exit load");
      return;
    }
    int elemNum = rowMeta.getElementNum();
    LongLongElement element = new LongLongElement();
    for (int i = 0; i < elemNum; i++) {
      load(element, in);
      row.set(element.colId, element.value);
    }
  }
}
