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

import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.IntLongVector;
import com.tencent.angel.ml.math2.vector.LongDoubleVector;
import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.math2.vector.LongIntVector;
import com.tencent.angel.ml.math2.vector.LongLongVector;
import com.tencent.angel.model.ModelIOUtils;
import com.tencent.angel.model.PSMatrixLoadContext;
import com.tencent.angel.model.PSMatrixSaveContext;
import com.tencent.angel.model.output.element.IntDoubleElement;
import com.tencent.angel.model.output.element.IntFloatElement;
import com.tencent.angel.model.output.element.IntIntElement;
import com.tencent.angel.model.output.element.IntLongElement;
import com.tencent.angel.model.output.element.LongDoubleElement;
import com.tencent.angel.model.output.element.LongFloatElement;
import com.tencent.angel.model.output.element.LongIntElement;
import com.tencent.angel.model.output.element.LongLongElement;
import com.tencent.angel.model.output.format.ElementFormat;
import com.tencent.angel.model.output.format.MatrixPartitionMeta;
import com.tencent.angel.model.output.format.RowPartitionMeta;
import com.tencent.angel.ps.storage.matrix.PartitionSource;
import com.tencent.angel.ps.storage.matrix.PartitionState;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;


/**
 * Loader/Saver use row-base matrix, it masks the details of all row vector traversal and only be
 * used in PS
 */
public class PSRowElementLoaderSaver extends PSMatrixLoaderSaverImpl {

  private final static Log LOG = LogFactory.getLog(PSRowLoaderSaver.class);
  private final ElementFormat format;

  public PSRowElementLoaderSaver(ElementFormat format, Configuration conf) {
    super(conf);
    this.format = format;
  }

  /**
   * Matrix partition data
   *
   * @param part matrix partition
   * @param partMeta matrix partition data meta
   * @param saveContext save context
   * @param output output stream
   */
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

    FSDataOutputStream dataOutputStream =
        new FSDataOutputStream(output, null, partMeta != null ? partMeta.getOffset() : 0);

    partMeta.setSaveRowNum(rowIds.size());
    for (int rowId : rowIds) {
      ServerRow row = rowPart.getRow(rowId);
      RowPartitionMeta rowMeta = new RowPartitionMeta(rowId, 0, 0);
      if (row != null) {
        rowMeta.setElementNum(row.size());
        rowMeta.setOffset(dataOutputStream.getPos());
        save(rowPart.getRow(rowId), saveContext, partMeta, output);
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
   * @param row row data
   * @param saveContext save context
   * @param meta partition data meta
   * @param out output stream
   */
  public void save(ServerRow row, PSMatrixSaveContext saveContext, MatrixPartitionMeta meta,
      DataOutputStream out) throws IOException {
    if (saveContext.cloneFirst()) {
      row = (ServerRow) row.adaptiveClone();
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
   * @param part matrix partition
   * @param partMeta matrix partition data meta
   * @param loadContext load context
   * @param input input stream
   */
  public void load(ServerPartition part, MatrixPartitionMeta partMeta,
      PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {
    RowBasedPartition rowPart = (RowBasedPartition) part;

    try {
      Map<Integer, RowPartitionMeta> rowMetas = partMeta.getRowMetas();
      for (RowPartitionMeta rowMeta : rowMetas.values()) {
        ServerRow row = rowPart.getRow(rowMeta.getRowId());
        load(row, partMeta, loadContext, input);
      }
    } finally {
      part.setState(PartitionState.READ_AND_WRITE);
    }
  }

  /**
   * Load a row data
   *
   * @param row row partition
   * @param meta partition meta
   * @param loadContext load context
   * @param in input stream
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
    IntFloatVector vector = ServerRowUtils.getVector(row);
    IntFloatElement element = new IntFloatElement();
    if (vector.isDense()) {
      float[] data = vector.getStorage().getValues();
      for (int i = 0; i < data.length; i++) {
        element.rowId = row.getRowId();
        element.colId = startCol + i;
        element.value = data[i];
        format.save(element, out);
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
          format.save(element, out);
        }
      } else {
        ObjectIterator<Int2FloatMap.Entry> iter = vector.getStorage().entryIterator();
        Int2FloatMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          element.rowId = row.getRowId();
          element.colId = entry.getIntKey() + startCol;
          element.value = entry.getFloatValue();
          format.save(element, out);
        }
      }
    }
  }

  private void save(ServerIntDoubleRow row, PSMatrixSaveContext saveContext,
      MatrixPartitionMeta meta, DataOutputStream out) throws IOException {
    int startCol = (int) meta.getStartCol();
    IntDoubleVector vector = ServerRowUtils.getVector(row);
    IntDoubleElement element = new IntDoubleElement();
    if (vector.isDense()) {
      double[] data = vector.getStorage().getValues();
      for (int i = 0; i < data.length; i++) {
        element.rowId = row.getRowId();
        element.colId = startCol + i;
        element.value = data[i];
        format.save(element, out);
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
          format.save(element, out);
        }
      } else {
        ObjectIterator<Int2DoubleMap.Entry> iter = vector.getStorage().entryIterator();
        Int2DoubleMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          element.rowId = row.getRowId();
          element.colId = entry.getIntKey() + startCol;
          element.value = entry.getDoubleValue();
          format.save(element, out);
        }
      }
    }
  }

  private void save(ServerIntIntRow row, PSMatrixSaveContext saveContext, MatrixPartitionMeta meta,
      DataOutputStream out) throws IOException {
    int startCol = (int) meta.getStartCol();
    IntIntVector vector = ServerRowUtils.getVector(row);
    IntIntElement element = new IntIntElement();
    if (vector.isDense()) {
      int[] data = vector.getStorage().getValues();
      for (int i = 0; i < data.length; i++) {
        element.rowId = row.getRowId();
        element.colId = startCol + i;
        element.value = data[i];
        format.save(element, out);
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
          format.save(element, out);
        }
      } else {
        ObjectIterator<Int2IntMap.Entry> iter = vector.getStorage().entryIterator();
        Int2IntMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          element.rowId = row.getRowId();
          element.colId = entry.getIntKey() + startCol;
          element.value = entry.getIntValue();
          format.save(element, out);
        }
      }
    }
  }

  private void save(ServerIntLongRow row, PSMatrixSaveContext saveContext, MatrixPartitionMeta meta,
      DataOutputStream out) throws IOException {
    int startCol = (int) meta.getStartCol();
    IntLongVector vector = ServerRowUtils.getVector(row);
    IntLongElement element = new IntLongElement();
    if (vector.isDense()) {
      long[] data = vector.getStorage().getValues();
      for (int i = 0; i < data.length; i++) {
        element.rowId = row.getRowId();
        element.colId = startCol + i;
        element.value = data[i];
        format.save(element, out);
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
          format.save(element, out);
        }
      } else {
        ObjectIterator<Int2LongMap.Entry> iter = vector.getStorage().entryIterator();
        Int2LongMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          element.rowId = row.getRowId();
          element.colId = entry.getIntKey() + startCol;
          element.value = entry.getLongValue();
          format.save(element, out);
        }
      }
    }
  }

  private void save(ServerLongDoubleRow row, PSMatrixSaveContext saveContext,
      MatrixPartitionMeta meta, DataOutputStream out) throws IOException {
    long startCol = meta.getStartCol();
    LongDoubleElement element = new LongDoubleElement();

    if (ServerRowUtils.getVector(row) instanceof IntDoubleVector) {
      IntDoubleVector vector = (IntDoubleVector) ServerRowUtils.getVector(row);
      if (vector.isDense()) {
        double[] data = vector.getStorage().getValues();
        for (int i = 0; i < data.length; i++) {
          element.rowId = row.getRowId();
          element.colId = startCol + i;
          element.value = data[i];
          format.save(element, out);
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
            format.save(element, out);
          }
        } else {
          ObjectIterator<Int2DoubleMap.Entry> iter = vector.getStorage().entryIterator();
          Int2DoubleMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            element.rowId = row.getRowId();
            element.colId = entry.getIntKey() + startCol;
            element.value = entry.getDoubleValue();
            format.save(element, out);
          }
        }
      }
    } else {
      LongDoubleVector vector = (LongDoubleVector) ServerRowUtils.getVector(row);
      if (saveContext.sortFirst()) {
        long[] indices = vector.getStorage().getIndices();
        double[] values = vector.getStorage().getValues();
        Sort.quickSort(indices, values, 0, indices.length - 1);
        for (int i = 0; i < indices.length; i++) {
          element.rowId = row.getRowId();
          element.colId = indices[i] + startCol;
          element.value = values[i];
          format.save(element, out);
        }
      } else {
        ObjectIterator<Long2DoubleMap.Entry> iter = vector.getStorage().entryIterator();
        Long2DoubleMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          element.rowId = row.getRowId();
          element.colId = entry.getLongKey() + startCol;
          element.value = entry.getDoubleValue();
          format.save(element, out);
        }
      }
    }
  }

  private void save(ServerLongFloatRow row, PSMatrixSaveContext saveContext,
      MatrixPartitionMeta meta, DataOutputStream out) throws IOException {
    long startCol = meta.getStartCol();
    LongFloatElement element = new LongFloatElement();
    if (ServerRowUtils.getVector(row) instanceof IntFloatVector) {
      IntFloatVector vector = (IntFloatVector) ServerRowUtils.getVector(row);
      if (vector.isDense()) {
        float[] data = vector.getStorage().getValues();
        for (int i = 0; i < data.length; i++) {
          element.rowId = row.getRowId();
          element.colId = startCol + i;
          element.value = data[i];
          format.save(element, out);
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
            format.save(element, out);
          }
        } else {
          ObjectIterator<Int2FloatMap.Entry> iter = vector.getStorage().entryIterator();
          Int2FloatMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            element.rowId = row.getRowId();
            element.colId = entry.getIntKey() + startCol;
            element.value = entry.getFloatValue();
            format.save(element, out);
          }
        }
      }
    } else {
      LongFloatVector vector = (LongFloatVector) ServerRowUtils.getVector(row);
      if (saveContext.sortFirst()) {
        long[] indices = vector.getStorage().getIndices();
        float[] values = vector.getStorage().getValues();
        Sort.quickSort(indices, values, 0, indices.length - 1);
        for (int i = 0; i < indices.length; i++) {
          element.rowId = row.getRowId();
          element.colId = indices[i] + startCol;
          element.value = values[i];
          format.save(element, out);
        }
      } else {
        ObjectIterator<Long2FloatMap.Entry> iter = vector.getStorage().entryIterator();
        Long2FloatMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          element.rowId = row.getRowId();
          element.colId = entry.getLongKey() + startCol;
          element.value = entry.getFloatValue();
          format.save(element, out);
        }
      }
    }
  }


  private void save(ServerLongIntRow row, PSMatrixSaveContext saveContext, MatrixPartitionMeta meta,
      DataOutputStream out) throws IOException {
    long startCol = meta.getStartCol();
    LongIntElement element = new LongIntElement();
    if (ServerRowUtils.getVector(row) instanceof IntIntVector) {
      IntIntVector vector = (IntIntVector) ServerRowUtils.getVector(row);
      if (vector.isDense()) {
        int[] data = vector.getStorage().getValues();
        for (int i = 0; i < data.length; i++) {
          element.rowId = row.getRowId();
          element.colId = startCol + i;
          element.value = data[i];
          format.save(element, out);
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
            format.save(element, out);
          }
        } else {
          ObjectIterator<Int2IntMap.Entry> iter = vector.getStorage().entryIterator();
          Int2IntMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            element.rowId = row.getRowId();
            element.colId = entry.getIntKey() + startCol;
            element.value = entry.getIntValue();
            format.save(element, out);
          }
        }
      }
    } else {
      LongIntVector vector = (LongIntVector) ServerRowUtils.getVector(row);
      if (saveContext.sortFirst()) {
        long[] indices = vector.getStorage().getIndices();
        int[] values = vector.getStorage().getValues();
        Sort.quickSort(indices, values, 0, indices.length - 1);
        for (int i = 0; i < indices.length; i++) {
          element.rowId = row.getRowId();
          element.colId = indices[i] + startCol;
          element.value = values[i];
          format.save(element, out);
        }
      } else {
        ObjectIterator<Long2IntMap.Entry> iter = vector.getStorage().entryIterator();
        Long2IntMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          element.rowId = row.getRowId();
          element.colId = entry.getLongKey() + startCol;
          element.value = entry.getIntValue();
          format.save(element, out);
        }
      }
    }
  }

  private void save(ServerLongLongRow row, PSMatrixSaveContext saveContext,
      MatrixPartitionMeta meta, DataOutputStream out) throws IOException {
    long startCol = meta.getStartCol();
    LongLongElement element = new LongLongElement();
    if (ServerRowUtils.getVector(row) instanceof IntLongVector) {
      IntLongVector vector = (IntLongVector) ServerRowUtils.getVector(row);
      if (vector.isDense()) {
        long[] data = vector.getStorage().getValues();
        for (int i = 0; i < data.length; i++) {
          element.rowId = row.getRowId();
          element.colId = startCol + i;
          element.value = data[i];
          format.save(element, out);
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
            format.save(element, out);
          }
        } else {
          ObjectIterator<Int2LongMap.Entry> iter = vector.getStorage().entryIterator();
          Int2LongMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            element.rowId = row.getRowId();
            element.colId = entry.getIntKey() + startCol;
            element.value = entry.getLongValue();
            format.save(element, out);
          }
        }
      }
    } else {
      LongLongVector vector = (LongLongVector) ServerRowUtils.getVector(row);
      if (saveContext.sortFirst()) {
        long[] indices = vector.getStorage().getIndices();
        long[] values = vector.getStorage().getValues();
        Sort.quickSort(indices, values, 0, indices.length - 1);
        for (int i = 0; i < indices.length; i++) {
          element.rowId = row.getRowId();
          element.colId = indices[i] + startCol;
          element.value = values[i];
          format.save(element, out);
        }
      } else {
        ObjectIterator<Long2LongMap.Entry> iter = vector.getStorage().entryIterator();
        Long2LongMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          element.rowId = row.getRowId();
          element.colId = entry.getLongKey() + startCol;
          element.value = entry.getLongValue();
          format.save(element, out);
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
      element.colId = i + (int) row.getStartCol();
      format.load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(ServerIntDoubleRow row, PSMatrixLoadContext loadContext,
      MatrixPartitionMeta meta, DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();
    IntDoubleElement element = new IntDoubleElement();
    for (int i = 0; i < elemNum; i++) {
      element.colId = i + (int) row.getStartCol();
      format.load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(ServerIntIntRow row, PSMatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();
    IntIntElement element = new IntIntElement();
    for (int i = 0; i < elemNum; i++) {
      element.colId = i + (int) row.getStartCol();
      format.load(element, in);
      row.set(element.colId, element.value);
    }
  }

  private void load(ServerIntLongRow row, PSMatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();
    IntLongElement element = new IntLongElement();
    for (int i = 0; i < elemNum; i++) {
      element.colId = i + (int) row.getStartCol();
      format.load(element, in);
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
      format.load(element, in);
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
      format.load(element, in);
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
      format.load(element, in);
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
      format.load(element, in);
      row.set(element.colId, element.value);
    }
  }
}
