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
import com.tencent.angel.ps.storage.matrix.PartitionState;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.partition.storage.ServerRowsStorage;
import com.tencent.angel.ps.storage.vector.ServerIntAnyRow;
import com.tencent.angel.ps.storage.vector.ServerIntDoubleRow;
import com.tencent.angel.ps.storage.vector.ServerIntFloatRow;
import com.tencent.angel.ps.storage.vector.ServerIntIntRow;
import com.tencent.angel.ps.storage.vector.ServerIntLongRow;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.ServerLongDoubleRow;
import com.tencent.angel.ps.storage.vector.ServerLongFloatRow;
import com.tencent.angel.ps.storage.vector.ServerLongIntRow;
import com.tencent.angel.ps.storage.vector.ServerLongLongRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.ServerRowUtils;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;

/**
 * Snapshot format, it just use for snapshot now.
 */
public class SnapshotFormat implements Format {
  protected final Configuration conf;
  private final static Log LOG = LogFactory.getLog(RowFormat.class);

  public SnapshotFormat(Configuration conf) {
    this.conf = conf;
  }


  public void save(ServerPartition part, MatrixPartitionMeta partMeta,
      PSMatrixSaveContext saveContext, DataOutputStream output) throws IOException {
    if(part instanceof RowBasedPartition) {
      save((RowBasedPartition) part, partMeta, saveContext, output);
    }
  }


  public void load(ServerPartition part, MatrixPartitionMeta partMeta,
      PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {
    if(part instanceof RowBasedPartition) {
      load((RowBasedPartition) part, partMeta, loadContext, input);
    }
  }

  static enum SaveType {
    DENSE(0), SPARSE(1);
    public static Map<Integer, SaveType> typeIdToTypeMap;

    static {
      typeIdToTypeMap = new HashMap<Integer, SaveType>();
      typeIdToTypeMap.put(DENSE.typeId, DENSE);
      typeIdToTypeMap.put(SPARSE.typeId, SPARSE);
    }

    public static SaveType valueOf(int id) {
      return typeIdToTypeMap.get(id);
    }

    private final int typeId;

    SaveType(int typeId) {
      this.typeId = typeId;
    }

    public int getTypeId() {
      return typeId;
    }
  }

  /**
   * Matrix partition data
   *
   * @param part matrix partition
   * @param partMeta matrix partition data meta
   * @param saveContext save context
   * @param output output stream
   */
  public void save(RowBasedPartition part, MatrixPartitionMeta partMeta,
      PSMatrixSaveContext saveContext, DataOutputStream output) throws IOException {
    List<Integer> rowIds = saveContext.getRowIndexes();
    ServerRowsStorage rows = part.getRowsStorage();
    if (rowIds == null || rowIds.isEmpty()) {
      int rowStart = part.getPartitionKey().getStartRow();
      int rowEnd = part.getPartitionKey().getEndRow();

      rowIds = new ArrayList<>(rowEnd - rowStart);
      for (int i = rowStart; i < rowEnd; i++) {
        rowIds.add(i);
      }
    } else {
      rowIds = ModelIOUtils.filter(part, rowIds);
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
        if (row.isDense()) {
          rowMeta.setSaveType(SaveType.DENSE.getTypeId());
        } else {
          rowMeta.setSaveType(SaveType.SPARSE.getTypeId());
        }
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
      } else if (row instanceof ServerIntAnyRow) {
        save((ServerIntAnyRow) row, saveContext, meta, out);
      } else if (row instanceof ServerLongAnyRow) {
        save((ServerLongAnyRow) row, saveContext, meta, out);
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
  public void load(RowBasedPartition part, MatrixPartitionMeta partMeta,
      PSMatrixLoadContext loadContext, DataInputStream input) throws IOException {
    ServerRowsStorage rows = part.getRowsStorage();
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
      } else if (row instanceof ServerIntAnyRow) {
        load((ServerIntAnyRow) row, loadContext, meta, in);
      } else if (row instanceof ServerLongAnyRow) {
        load((ServerLongAnyRow) row, loadContext, meta, in);
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
    if (vector.isDense()) {
      float[] data = vector.getStorage().getValues();
      for (int i = 0; i < data.length; i++) {
        out.writeFloat(data[i]);
      }
    } else if (vector.isSorted()) {
      int[] indices = vector.getStorage().getIndices();
      float[] values = vector.getStorage().getValues();
      for (int i = 0; i < indices.length; i++) {
        out.writeInt(indices[i] + startCol);
        out.writeFloat(values[i]);
      }
    } else {
      ObjectIterator<Int2FloatMap.Entry> iter = vector.getStorage().entryIterator();
      Int2FloatMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        out.writeInt(entry.getIntKey() + startCol);
        out.writeFloat(entry.getFloatValue());
      }
    }
  }

  private void save(ServerIntDoubleRow row, PSMatrixSaveContext saveContext,
      MatrixPartitionMeta meta, DataOutputStream out) throws IOException {
    int startCol = (int) meta.getStartCol();
    IntDoubleVector vector = ServerRowUtils.getVector(row);
    if (vector.isDense()) {
      double[] data = vector.getStorage().getValues();
      for (int i = 0; i < data.length; i++) {
        out.writeDouble(data[i]);
      }
    } else if (vector.isSorted()) {
      int[] indices = vector.getStorage().getIndices();
      double[] values = vector.getStorage().getValues();
      for (int i = 0; i < indices.length; i++) {
        out.writeInt(indices[i] + startCol);
        out.writeDouble(values[i]);
      }
    } else {
      ObjectIterator<Int2DoubleMap.Entry> iter = vector.getStorage().entryIterator();
      Int2DoubleMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        out.writeInt(entry.getIntKey() + startCol);
        out.writeDouble(entry.getDoubleValue());
      }
    }
  }

  private void save(ServerIntIntRow row, PSMatrixSaveContext saveContext, MatrixPartitionMeta meta,
      DataOutputStream out) throws IOException {
    int startCol = (int) meta.getStartCol();
    IntIntVector vector = ServerRowUtils.getVector(row);
    if (vector.isDense()) {
      int[] data = vector.getStorage().getValues();
      for (int i = 0; i < data.length; i++) {
        out.writeInt(data[i]);
      }
    } else if (vector.isSorted()) {
      int[] indices = vector.getStorage().getIndices();
      int[] values = vector.getStorage().getValues();
      for (int i = 0; i < indices.length; i++) {
        out.writeInt(indices[i] + startCol);
        out.writeInt(values[i]);
      }
    } else {
      ObjectIterator<Int2IntMap.Entry> iter = vector.getStorage().entryIterator();
      Int2IntMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        out.writeInt(entry.getIntKey() + startCol);
        out.writeInt(entry.getIntValue());
      }
    }
  }

  private void save(ServerIntLongRow row, PSMatrixSaveContext saveContext, MatrixPartitionMeta meta,
      DataOutputStream out) throws IOException {
    int startCol = (int) meta.getStartCol();
    IntLongVector vector = ServerRowUtils.getVector(row);
    if (vector.isDense()) {
      long[] data = vector.getStorage().getValues();
      for (int i = 0; i < data.length; i++) {
        out.writeLong(data[i]);
      }
    } else if (vector.isSorted()) {
      int[] indices = vector.getStorage().getIndices();
      long[] values = vector.getStorage().getValues();
      for (int i = 0; i < indices.length; i++) {
        out.writeInt(indices[i] + startCol);
        out.writeLong(values[i]);
      }
    } else {
      ObjectIterator<Int2LongMap.Entry> iter = vector.getStorage().entryIterator();
      Int2LongMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        out.writeInt(entry.getIntKey() + startCol);
        out.writeLong(entry.getLongValue());
      }
    }
  }

  private void save(ServerLongDoubleRow row, PSMatrixSaveContext saveContext,
      MatrixPartitionMeta meta, DataOutputStream out) throws IOException {
    long startCol = meta.getStartCol();
    if (ServerRowUtils.getVector(row) instanceof IntDoubleVector) {
      IntDoubleVector vector = (IntDoubleVector) ServerRowUtils.getVector(row);
      if (vector.isDense()) {
        double[] data = vector.getStorage().getValues();
        for (int i = 0; i < data.length; i++) {
          out.writeDouble(data[i]);
        }
      } else if (vector.isSorted()) {
        int[] indices = vector.getStorage().getIndices();
        double[] values = vector.getStorage().getValues();
        for (int i = 0; i < indices.length; i++) {
          out.writeLong(indices[i] + startCol);
          out.writeDouble(values[i]);
        }
      } else {
        ObjectIterator<Int2DoubleMap.Entry> iter = vector.getStorage().entryIterator();
        Int2DoubleMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          out.writeLong(entry.getIntKey() + startCol);
          out.writeDouble(entry.getDoubleValue());
        }
      }
    } else {
      LongDoubleVector vector = (LongDoubleVector) ServerRowUtils.getVector(row);
      if (vector.isSorted()) {
        long[] indices = vector.getStorage().getIndices();
        double[] values = vector.getStorage().getValues();
        for (int i = 0; i < indices.length; i++) {
          out.writeLong(indices[i] + startCol);
          out.writeDouble(values[i]);
        }
      } else {
        ObjectIterator<Long2DoubleMap.Entry> iter = vector.getStorage().entryIterator();
        Long2DoubleMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          out.writeLong(entry.getLongKey() + startCol);
          out.writeDouble(entry.getDoubleValue());
        }
      }
    }
  }

  private void save(ServerLongFloatRow row, PSMatrixSaveContext saveContext,
      MatrixPartitionMeta meta, DataOutputStream out) throws IOException {
    long startCol = meta.getStartCol();
    if (ServerRowUtils.getVector(row) instanceof IntFloatVector) {
      IntFloatVector vector = (IntFloatVector) ServerRowUtils.getVector(row);
      if (vector.isDense()) {
        float[] data = vector.getStorage().getValues();
        for (int i = 0; i < data.length; i++) {
          out.writeFloat(data[i]);
        }
      } else if (vector.isSorted()) {
        int[] indices = vector.getStorage().getIndices();
        float[] values = vector.getStorage().getValues();
        for (int i = 0; i < indices.length; i++) {
          out.writeLong(indices[i] + startCol);
          out.writeFloat(values[i]);
        }
      } else {
        ObjectIterator<Int2FloatMap.Entry> iter = vector.getStorage().entryIterator();
        Int2FloatMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          out.writeLong(entry.getIntKey() + startCol);
          out.writeFloat(entry.getFloatValue());
        }
      }
    } else {
      LongFloatVector vector = (LongFloatVector) ServerRowUtils.getVector(row);
      if (vector.isSorted()) {
        long[] indices = vector.getStorage().getIndices();
        float[] values = vector.getStorage().getValues();
        for (int i = 0; i < indices.length; i++) {
          out.writeLong(indices[i] + startCol);
          out.writeFloat(values[i]);
        }
      } else {
        ObjectIterator<Long2FloatMap.Entry> iter = vector.getStorage().entryIterator();
        Long2FloatMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          out.writeLong(entry.getLongKey() + startCol);
          out.writeFloat(entry.getFloatValue());
        }
      }
    }
  }


  private void save(ServerLongIntRow row, PSMatrixSaveContext saveContext, MatrixPartitionMeta meta,
      DataOutputStream out) throws IOException {
    long startCol = meta.getStartCol();
    if (ServerRowUtils.getVector(row) instanceof IntIntVector) {
      IntIntVector vector = (IntIntVector) ServerRowUtils.getVector(row);
      if (vector.isDense()) {
        int[] data = vector.getStorage().getValues();
        for (int i = 0; i < data.length; i++) {
          out.writeInt(data[i]);
        }
      } else if (vector.isSorted()) {
        int[] indices = vector.getStorage().getIndices();
        int[] values = vector.getStorage().getValues();
        for (int i = 0; i < indices.length; i++) {
          out.writeLong(indices[i] + startCol);
          out.writeInt(values[i]);
        }
      } else {
        ObjectIterator<Int2IntMap.Entry> iter = vector.getStorage().entryIterator();
        Int2IntMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          out.writeLong(entry.getIntKey() + startCol);
          out.writeInt(entry.getIntValue());
        }
      }
    } else {
      LongIntVector vector = (LongIntVector) ServerRowUtils.getVector(row);
      if (vector.isSorted()) {
        long[] indices = vector.getStorage().getIndices();
        int[] values = vector.getStorage().getValues();
        for (int i = 0; i < indices.length; i++) {
          out.writeLong(indices[i] + startCol);
          out.writeInt(values[i]);
        }
      } else {
        ObjectIterator<Long2IntMap.Entry> iter = vector.getStorage().entryIterator();
        Long2IntMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          out.writeLong(entry.getLongKey() + startCol);
          out.writeInt(entry.getIntValue());
        }
      }
    }
  }

  private void save(ServerLongLongRow row, PSMatrixSaveContext saveContext,
      MatrixPartitionMeta meta, DataOutputStream out) throws IOException {
    long startCol = meta.getStartCol();
    if (ServerRowUtils.getVector(row) instanceof IntLongVector) {
      IntLongVector vector = (IntLongVector) ServerRowUtils.getVector(row);
      if (vector.isDense()) {
        long[] data = vector.getStorage().getValues();
        for (int i = 0; i < data.length; i++) {
          out.writeLong(data[i]);
        }
      } else if (vector.isSorted()) {
        int[] indices = vector.getStorage().getIndices();
        long[] values = vector.getStorage().getValues();
        for (int i = 0; i < indices.length; i++) {
          out.writeLong(indices[i] + startCol);
          out.writeLong(values[i]);
        }
      } else {
        ObjectIterator<Int2LongMap.Entry> iter = vector.getStorage().entryIterator();
        Int2LongMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          out.writeLong(entry.getIntKey() + startCol);
          out.writeLong(entry.getLongValue());
        }
      }
    } else {
      LongLongVector vector = (LongLongVector) ServerRowUtils.getVector(row);
      if (vector.isSorted()) {
        long[] indices = vector.getStorage().getIndices();
        long[] values = vector.getStorage().getValues();
        for (int i = 0; i < indices.length; i++) {
          out.writeLong(indices[i] + startCol);
          out.writeLong(values[i]);
        }
      } else {
        ObjectIterator<Long2LongMap.Entry> iter = vector.getStorage().entryIterator();
        Long2LongMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          out.writeLong(entry.getLongKey() + startCol);
          out.writeLong(entry.getLongValue());
        }
      }
    }
  }

  private void save(ServerIntAnyRow row, PSMatrixSaveContext saveContext,
      MatrixPartitionMeta meta, DataOutputStream output) throws IOException {
    row.getStorage().serialize(output);
  }

  private void save(ServerLongAnyRow row, PSMatrixSaveContext saveContext,
      MatrixPartitionMeta meta, DataOutputStream output) throws IOException {
    row.getStorage().serialize(output);
  }

  private void load(ServerIntAnyRow row, PSMatrixLoadContext loadContext,
      MatrixPartitionMeta meta, DataInputStream in) throws IOException {
    row.getStorage().deserialize(in);
  }

  private void load(ServerLongAnyRow row, PSMatrixLoadContext loadContext,
      MatrixPartitionMeta meta, DataInputStream in) throws IOException {
    row.getStorage().deserialize(in);
  }

  private void load(ServerIntFloatRow row, PSMatrixLoadContext loadContext,
      MatrixPartitionMeta meta, DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();
    SaveType saveType = SaveType.valueOf(rowMeta.getSaveType());
    int startCol = (int) meta.getStartCol();
    if (saveType == SaveType.DENSE) {
      for (int i = 0; i < elemNum; i++) {
        row.set(i + startCol, in.readFloat());
      }
    } else {
      for (int i = 0; i < elemNum; i++) {
        row.set(in.readInt(), in.readFloat());
      }
    }
  }

  private void load(ServerIntDoubleRow row, PSMatrixLoadContext loadContext,
      MatrixPartitionMeta meta, DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();
    SaveType saveType = SaveType.valueOf(rowMeta.getSaveType());
    int startCol = (int) meta.getStartCol();
    if (saveType == SaveType.DENSE) {
      for (int i = 0; i < elemNum; i++) {
        row.set(i + startCol, in.readDouble());
      }
    } else {
      for (int i = 0; i < elemNum; i++) {
        row.set(in.readInt(), in.readDouble());
      }
    }
  }

  private void load(ServerIntIntRow row, PSMatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();
    SaveType saveType = SaveType.valueOf(rowMeta.getSaveType());
    int startCol = (int) meta.getStartCol();
    if (saveType == SaveType.DENSE) {
      for (int i = 0; i < elemNum; i++) {
        row.set(i + startCol, in.readInt());
      }
    } else {
      for (int i = 0; i < elemNum; i++) {
        row.set(in.readInt(), in.readInt());
      }
    }
  }

  private void load(ServerIntLongRow row, PSMatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();
    SaveType saveType = SaveType.valueOf(rowMeta.getSaveType());
    int startCol = (int) meta.getStartCol();
    if (saveType == SaveType.DENSE) {
      for (int i = 0; i < elemNum; i++) {
        row.set(i + startCol, in.readLong());
      }
    } else {
      for (int i = 0; i < elemNum; i++) {
        row.set(in.readInt(), in.readLong());
      }
    }
  }

  private void load(ServerLongFloatRow row, PSMatrixLoadContext loadContext,
      MatrixPartitionMeta meta, DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();
    SaveType saveType = SaveType.valueOf(rowMeta.getSaveType());
    long startCol = meta.getStartCol();
    if (saveType == SaveType.DENSE) {
      for (int i = 0; i < elemNum; i++) {
        row.set(i + startCol, in.readFloat());
      }
    } else {
      for (int i = 0; i < elemNum; i++) {
        row.set(in.readLong(), in.readFloat());
      }
    }
  }

  private void load(ServerLongDoubleRow row, PSMatrixLoadContext loadContext,
      MatrixPartitionMeta meta, DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();
    SaveType saveType = SaveType.valueOf(rowMeta.getSaveType());
    long startCol = meta.getStartCol();
    if (saveType == SaveType.DENSE) {
      for (int i = 0; i < elemNum; i++) {
        row.set(i + startCol, in.readDouble());
      }
    } else {
      for (int i = 0; i < elemNum; i++) {
        row.set(in.readLong(), in.readDouble());
      }
    }
  }

  private void load(ServerLongIntRow row, PSMatrixLoadContext loadContext, MatrixPartitionMeta meta,
      DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();
    SaveType saveType = SaveType.valueOf(rowMeta.getSaveType());
    long startCol = meta.getStartCol();
    if (saveType == SaveType.DENSE) {
      for (int i = 0; i < elemNum; i++) {
        row.set(i + startCol, in.readInt());
      }
    } else {
      for (int i = 0; i < elemNum; i++) {
        row.set(in.readLong(), in.readInt());
      }
    }
  }

  private void load(ServerLongLongRow row, PSMatrixLoadContext loadContext,
      MatrixPartitionMeta meta, DataInputStream in) throws IOException {
    RowPartitionMeta rowMeta = meta.getRowMeta(row.getRowId());
    int elemNum = rowMeta.getElementNum();
    SaveType saveType = SaveType.valueOf(rowMeta.getSaveType());
    long startCol = meta.getStartCol();
    if (saveType == SaveType.DENSE) {
      for (int i = 0; i < elemNum; i++) {
        row.set(i + startCol, in.readLong());
      }
    } else {
      for (int i = 0; i < elemNum; i++) {
        row.set(in.readLong(), in.readLong());
      }
    }
  }
}
