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
import com.tencent.angel.ml.math2.vector.*;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.UpdateItem;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2FloatMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.List;

public class RowsViewUpdateItem extends UpdateItem {
  private final Vector[] rows;
  private final PartitionKey partKey;
  private final long column;

  public RowsViewUpdateItem(PartitionKey partKey, Vector[] rows, long column) {
    this.partKey = partKey;
    this.rows = rows;
    this.column = column;
  }

  public RowsViewUpdateItem() {
    this(null, null, 0);
  }

  public PartitionKey getPartKey() {
    return partKey;
  }

  public Vector[] getRows() {
    return rows;
  }

  public long getColumn() {
    return column;
  }

  @Override public void serialize(ByteBuf buf) {
    int pos = buf.writerIndex();
    buf.writeInt(0);
    int rowNum = 0;
    for (int i = 0; i < rows.length; i++) {
      if (rowInPart(rows[i].getRowId(), partKey)) {
        serializeRow(buf, rows[i]);
        rowNum++;
      }
    }
    buf.setInt(pos, rowNum);
  }

  private boolean rowInPart(int rowId, PartitionKey partKey) {
    return rowId >= partKey.getStartRow() && rowId < partKey.getEndRow();
  }

  @Override public void deserialize(ByteBuf buf) {

  }

  @Override public int bufferLen() {
    int len = 4;
    for (int i = 0; i < rows.length; i++) {
      if (rowInPart(rows[i].getRowId(), partKey)) {
        len += bufferLen(rows[i], partKey, column);
      }
    }
    return len;
  }

  private int bufferLen(Vector row, PartitionKey partKey, long column) {
    final boolean needCheck = (column != (partKey.getEndCol() - partKey.getStartCol()));
    int len = 4;
    switch (row.getType()) {
      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE: {
        len += bufferLen((IntDoubleVector) row, partKey, needCheck);
        break;
      }

      case T_DOUBLE_DENSE_COMPONENT:
      case T_DOUBLE_SPARSE_COMPONENT: {
        len += bufferLen((CompIntDoubleVector) row, partKey, false);
        break;
      }

      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE: {
        len += bufferLen((IntFloatVector) row, partKey, needCheck);
        break;
      }

      case T_FLOAT_DENSE_COMPONENT:
      case T_FLOAT_SPARSE_COMPONENT: {
        len += bufferLen((CompIntFloatVector) row, partKey, false);
        break;
      }

      case T_INT_DENSE:
      case T_INT_SPARSE: {
        len += bufferLen((IntIntVector) row, partKey, needCheck);
        break;
      }

      case T_INT_DENSE_COMPONENT:
      case T_INT_SPARSE_COMPONENT: {
        len += bufferLen((CompIntIntVector) row, partKey, false);
        break;
      }

      case T_LONG_DENSE:
      case T_LONG_SPARSE: {
        len += bufferLen((IntLongVector) row, partKey, needCheck);
        break;
      }

      case T_LONG_DENSE_COMPONENT:
      case T_LONG_SPARSE_COMPONENT: {
        len += bufferLen((CompIntLongVector) row, partKey, false);
        break;
      }

      case T_DOUBLE_SPARSE_LONGKEY: {
        len += bufferLen((LongDoubleVector) row, partKey, needCheck);
        break;
      }

      case T_DOUBLE_DENSE_LONGKEY_COMPONENT:
      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT: {
        len += bufferLen((CompLongDoubleVector) row, partKey, needCheck);
        break;
      }

      case T_FLOAT_SPARSE_LONGKEY: {
        len += bufferLen((LongFloatVector) row, partKey, needCheck);
        break;
      }

      case T_FLOAT_DENSE_LONGKEY_COMPONENT:
      case T_FLOAT_SPARSE_LONGKEY_COMPONENT: {
        len += bufferLen((CompLongFloatVector) row, partKey, needCheck);
        break;
      }

      case T_INT_SPARSE_LONGKEY: {
        len += bufferLen((LongIntVector) row, partKey, needCheck);
        break;
      }

      case T_INT_DENSE_LONGKEY_COMPONENT:
      case T_INT_SPARSE_LONGKEY_COMPONENT: {
        len += bufferLen((CompLongIntVector) row, partKey, needCheck);
        break;
      }

      case T_LONG_SPARSE_LONGKEY: {
        len += bufferLen((LongLongVector) row, partKey, needCheck);
        break;
      }

      case T_LONG_DENSE_LONGKEY_COMPONENT:
      case T_LONG_SPARSE_LONGKEY_COMPONENT: {
        len += bufferLen((CompLongLongVector) row, partKey, needCheck);
        break;
      }
    }

    return len;
  }

  private int bufferLen(IntDoubleVector row, PartitionKey partKey, boolean needCheck) {
    if (row.isDense()) {
      return 8 + 8 * ((int) (partKey.getEndCol() - partKey.getStartCol()));
    } else if (row.isSparse()) {
      if (needCheck) {
        return 8 + 12 * row.size();
      } else {
        int num = 0;
        ObjectIterator<Int2DoubleMap.Entry> iter = row.getStorage().entryIterator();
        Int2DoubleMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          if (!needCheck || colInPart(entry.getIntKey(), partKey)) {
            num++;
          }
        }
        return 8 + 12 * num;
      }
    } else {
      return 8 + 12 * count(row.getStorage().getIndices(), (int) partKey.getStartCol(),
        (int) partKey.getEndCol());
    }
  }

  private int bufferLen(IntFloatVector row, PartitionKey partKey, boolean needCheck) {
    if (row.isDense()) {
      return 8 + 4 * ((int) (partKey.getEndCol() - partKey.getStartCol()));
    } else if (row.isSparse()) {
      if (needCheck) {
        return 8 + 8 * row.size();
      } else {
        int num = 0;
        ObjectIterator<Int2FloatMap.Entry> iter = row.getStorage().entryIterator();
        Int2FloatMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          if (!needCheck || colInPart(entry.getIntKey(), partKey)) {
            num++;
          }
        }
        return 8 + 8 * num;
      }
    } else {
      return 8 + 8 * count(row.getStorage().getIndices(), (int) partKey.getStartCol(),
        (int) partKey.getEndCol());
    }
  }

  private int bufferLen(IntIntVector row, PartitionKey partKey, boolean needCheck) {
    if (row.isDense()) {
      return 8 + 4 * ((int) (partKey.getEndCol() - partKey.getStartCol()));
    } else if (row.isSparse()) {
      if (needCheck) {
        return 8 + 8 * row.size();
      } else {
        int num = 0;
        ObjectIterator<Int2IntMap.Entry> iter = row.getStorage().entryIterator();
        Int2IntMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          if (!needCheck || colInPart(entry.getIntKey(), partKey)) {
            num++;
          }
        }
        return 8 + 8 * num;
      }
    } else {
      return 8 + 8 * count(row.getStorage().getIndices(), (int) partKey.getStartCol(),
        (int) partKey.getEndCol());
    }
  }

  private int bufferLen(IntLongVector row, PartitionKey partKey, boolean needCheck) {
    if (row.isDense()) {
      return 8 + 4 * ((int) (partKey.getEndCol() - partKey.getStartCol()));
    } else if (row.isSparse()) {
      if (needCheck) {
        return 8 + 8 * row.size();
      } else {
        int num = 0;
        ObjectIterator<Int2LongMap.Entry> iter = row.getStorage().entryIterator();
        Int2LongMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          if (!needCheck || colInPart(entry.getIntKey(), partKey)) {
            num++;
          }
        }
        return 8 + 12 * num;
      }
    } else {
      return 8 + 12 * count(row.getStorage().getIndices(), (int) partKey.getStartCol(),
        (int) partKey.getEndCol());
    }
  }

  private int bufferLen(LongDoubleVector row, PartitionKey partKey, boolean needCheck) {
    if (row.isSparse()) {
      if (needCheck) {
        return (int) (8 + 16 * row.size());
      } else {
        int num = 0;
        ObjectIterator<Long2DoubleMap.Entry> iter = row.getStorage().entryIterator();
        Long2DoubleMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          if (!needCheck || colInPart(entry.getLongKey(), partKey)) {
            num++;
          }
        }
        return 8 + 16 * num;
      }
    } else {
      return 8 + 16 * count(row.getStorage().getIndices(), (int) partKey.getStartCol(),
        (int) partKey.getEndCol());
    }
  }

  private int bufferLen(LongFloatVector row, PartitionKey partKey, boolean needCheck) {
    if (row.isSparse()) {
      if (needCheck) {
        return (int) (8 + 12 * row.size());
      } else {
        int num = 0;
        ObjectIterator<Long2FloatMap.Entry> iter = row.getStorage().entryIterator();
        Long2FloatMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          if (!needCheck || colInPart(entry.getLongKey(), partKey)) {
            num++;
          }
        }
        return 8 + 12 * num;
      }
    } else {
      return 8 + 12 * count(row.getStorage().getIndices(), (int) partKey.getStartCol(),
        (int) partKey.getEndCol());
    }
  }

  private int bufferLen(LongIntVector row, PartitionKey partKey, boolean needCheck) {
    if (row.isSparse()) {
      if (needCheck) {
        return (int) (8 + 12 * row.size());
      } else {
        int num = 0;
        ObjectIterator<Long2IntMap.Entry> iter = row.getStorage().entryIterator();
        Long2IntMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          if (!needCheck || colInPart(entry.getLongKey(), partKey)) {
            num++;
          }
        }
        return 8 + 8 * num;
      }
    } else {
      return 8 + 8 * count(row.getStorage().getIndices(), (int) partKey.getStartCol(),
        (int) partKey.getEndCol());
    }
  }

  private int bufferLen(LongLongVector row, PartitionKey partKey, boolean needCheck) {
    if (row.isSparse()) {
      if (needCheck) {
        return (int) (8 + 16 * row.size());
      } else {
        int num = 0;
        ObjectIterator<Long2LongMap.Entry> iter = row.getStorage().entryIterator();
        Long2LongMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          if (!needCheck || colInPart(entry.getLongKey(), partKey)) {
            num++;
          }
        }
        return 8 + 16 * num;
      }
    } else {
      return 8 + 16 * count(row.getStorage().getIndices(), (int) partKey.getStartCol(),
        (int) partKey.getEndCol());
    }
  }

  private int bufferLen(CompIntDoubleVector row, PartitionKey partKey, boolean needCheck) {
    List<PartitionKey> parts =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(row.getMatrixId(), row.getRowId());
    IntDoubleVector[] partVecs = row.getPartitions();
    assert parts.size() == partVecs.length;
    for (int i = 0; i < partVecs.length; i++) {
      if (parts.get(i).getPartitionId() == partKey.getPartitionId()) {
        return bufferLen(partVecs[i], partKey, false);
      }
    }
    return 0;
  }

  private int bufferLen(CompIntFloatVector row, PartitionKey partKey, boolean needCheck) {
    List<PartitionKey> parts =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(row.getMatrixId(), row.getRowId());
    IntFloatVector[] partVecs = row.getPartitions();
    assert parts.size() == partVecs.length;
    for (int i = 0; i < partVecs.length; i++) {
      if (parts.get(i).getPartitionId() == partKey.getPartitionId()) {
        return bufferLen(partVecs[i], partKey, false);
      }
    }
    return 0;
  }

  private int bufferLen(CompIntIntVector row, PartitionKey partKey, boolean needCheck) {
    List<PartitionKey> parts =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(row.getMatrixId(), row.getRowId());
    IntIntVector[] partVecs = row.getPartitions();
    assert parts.size() == partVecs.length;
    for (int i = 0; i < partVecs.length; i++) {
      if (parts.get(i).getPartitionId() == partKey.getPartitionId()) {
        return bufferLen(partVecs[i], partKey, false);
      }
    }
    return 0;
  }

  private int bufferLen(CompIntLongVector row, PartitionKey partKey, boolean needCheck) {
    List<PartitionKey> parts =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(row.getMatrixId(), row.getRowId());
    IntLongVector[] partVecs = row.getPartitions();
    assert parts.size() == partVecs.length;
    for (int i = 0; i < partVecs.length; i++) {
      if (parts.get(i).getPartitionId() == partKey.getPartitionId()) {
        return bufferLen(partVecs[i], partKey, false);
      }
    }
    return 0;
  }

  private int bufferLen(CompLongDoubleVector row, PartitionKey partKey, boolean needCheck) {
    List<PartitionKey> parts =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(row.getMatrixId(), row.getRowId());
    LongDoubleVector[] partVecs = row.getPartitions();
    assert parts.size() == partVecs.length;
    for (int i = 0; i < partVecs.length; i++) {
      if (parts.get(i).getPartitionId() == partKey.getPartitionId()) {
        return bufferLen(partVecs[i], partKey, false);
      }
    }
    return 0;
  }

  private int bufferLen(CompLongFloatVector row, PartitionKey partKey, boolean needCheck) {
    List<PartitionKey> parts =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(row.getMatrixId(), row.getRowId());
    LongFloatVector[] partVecs = row.getPartitions();
    assert parts.size() == partVecs.length;
    for (int i = 0; i < partVecs.length; i++) {
      if (parts.get(i).getPartitionId() == partKey.getPartitionId()) {
        return bufferLen(partVecs[i], partKey, false);
      }
    }
    return 0;
  }

  private int bufferLen(CompLongIntVector row, PartitionKey partKey, boolean needCheck) {
    List<PartitionKey> parts =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(row.getMatrixId(), row.getRowId());
    LongIntVector[] partVecs = row.getPartitions();
    assert parts.size() == partVecs.length;
    for (int i = 0; i < partVecs.length; i++) {
      if (parts.get(i).getPartitionId() == partKey.getPartitionId()) {
        return bufferLen(partVecs[i], partKey, false);
      }
    }
    return 0;
  }

  private int bufferLen(CompLongLongVector row, PartitionKey partKey, boolean needCheck) {
    List<PartitionKey> parts =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(row.getMatrixId(), row.getRowId());
    LongLongVector[] partVecs = row.getPartitions();
    assert parts.size() == partVecs.length;
    for (int i = 0; i < partVecs.length; i++) {
      if (parts.get(i).getPartitionId() == partKey.getPartitionId()) {
        return bufferLen(partVecs[i], partKey, false);
      }
    }
    return 0;
  }

  //TODO
  private int count(int[] indices, int start, int end) {
    return 0;
  }

  //TODO
  private int count(long[] indices, long start, long end) {
    return 0;
  }

  private void serializeRow(ByteBuf buf, Vector row) {
    final boolean needCheck = (column != (partKey.getEndCol() - partKey.getStartCol()));
    buf.writeInt(row.getRowId());
    switch (row.getType()) {
      case T_DOUBLE_DENSE:
      case T_DOUBLE_SPARSE: {
        serializeIntDoubleRow((IntDoubleVector) row, buf, partKey, needCheck, false);
        break;
      }

      case T_DOUBLE_DENSE_COMPONENT:
      case T_DOUBLE_SPARSE_COMPONENT: {
        serializeIntDoubleCompRow((CompIntDoubleVector) row, buf, partKey, false);
        break;
      }

      case T_FLOAT_DENSE:
      case T_FLOAT_SPARSE: {
        serializeIntFloatRow((IntFloatVector) row, buf, partKey, needCheck, false);
        break;
      }

      case T_FLOAT_DENSE_COMPONENT:
      case T_FLOAT_SPARSE_COMPONENT: {
        serializeIntFloatCompRow((CompIntFloatVector) row, buf, partKey, false);
        break;
      }

      case T_INT_DENSE:
      case T_INT_SPARSE: {
        serializeIntIntRow((IntIntVector) row, buf, partKey, needCheck, false);
        break;
      }

      case T_INT_DENSE_COMPONENT:
      case T_INT_SPARSE_COMPONENT: {
        serializeIntIntCompRow((CompIntIntVector) row, buf, partKey, false);
        break;
      }

      case T_LONG_DENSE:
      case T_LONG_SPARSE: {
        serializeIntLongRow((IntLongVector) row, buf, partKey, needCheck, false);
        break;
      }

      case T_LONG_DENSE_COMPONENT:
      case T_LONG_SPARSE_COMPONENT: {
        serializeIntLongCompRow((CompIntLongVector) row, buf, partKey, false);
        break;
      }

      case T_DOUBLE_SPARSE_LONGKEY: {
        serializeLongDoubleRow((LongDoubleVector) row, buf, partKey, needCheck, false);
        break;
      }

      case T_DOUBLE_DENSE_LONGKEY_COMPONENT:
      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT: {
        serializeLongDoubleCompRow((CompLongDoubleVector) row, buf, partKey, false);
        break;
      }

      case T_FLOAT_SPARSE_LONGKEY: {
        serializeLongFloatRow((LongFloatVector) row, buf, partKey, needCheck, false);
        break;
      }

      case T_FLOAT_DENSE_LONGKEY_COMPONENT:
      case T_FLOAT_SPARSE_LONGKEY_COMPONENT: {
        serializeLongFloatCompRow((CompLongFloatVector) row, buf, partKey, false);
        break;
      }

      case T_INT_SPARSE_LONGKEY: {
        serializeLongIntRow((LongIntVector) row, buf, partKey, needCheck, false);
        break;
      }

      case T_INT_DENSE_LONGKEY_COMPONENT:
      case T_INT_SPARSE_LONGKEY_COMPONENT: {
        serializeLongIntCompRow((CompLongIntVector) row, buf, partKey, false);
        break;
      }

      case T_LONG_SPARSE_LONGKEY: {
        serializeLongLongRow((LongLongVector) row, buf, partKey, needCheck, false);
        break;
      }

      case T_LONG_DENSE_LONGKEY_COMPONENT:
      case T_LONG_SPARSE_LONGKEY_COMPONENT: {
        serializeLongLongCompRow((CompLongLongVector) row, buf, partKey, false);
        break;
      }
    }
  }

  private void serializeIntDoubleRow(IntDoubleVector row, ByteBuf buf, PartitionKey partKey) {
    if (row.isDense()) {
      buf.writeInt(RowType.T_DOUBLE_DENSE.getNumber());
      double[] values = row.getStorage().getValues();
      buf.writeInt(values.length);
      for (int i = 0; i < values.length; i++) {
        buf.writeDouble(values[i]);
      }
    } else if (row.isSparse()) {
      buf.writeInt(RowType.T_DOUBLE_SPARSE.getNumber());
      buf.writeInt(row.size());
      ObjectIterator<Int2DoubleMap.Entry> iter = row.getStorage().entryIterator();
      Int2DoubleMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeInt(entry.getIntKey());
        buf.writeDouble(entry.getDoubleValue());
      }
    } else {
      buf.writeInt(RowType.T_DOUBLE_SPARSE.getNumber());
      buf.writeInt(row.size());
      int[] indices = row.getStorage().getIndices();
      double[] values = row.getStorage().getValues();
      for (int i = 0; i < indices.length; i++) {
        buf.writeInt(indices[i]);
        buf.writeDouble(values[i]);
      }
    }
  }

  private void serializeIntFloatRow(IntFloatVector row, ByteBuf buf) {
    if (row.isDense()) {
      buf.writeInt(RowType.T_FLOAT_DENSE.getNumber());
      float[] values = row.getStorage().getValues();
      buf.writeInt(values.length);
      for (int i = 0; i < values.length; i++) {
        buf.writeFloat(values[i]);
      }
    } else if (row.isSparse()) {
      buf.writeInt(RowType.T_FLOAT_SPARSE.getNumber());
      buf.writeInt(row.size());
      ObjectIterator<Int2FloatMap.Entry> iter = row.getStorage().entryIterator();
      Int2FloatMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeInt(entry.getIntKey());
        buf.writeFloat(entry.getFloatValue());
      }
    } else {
      buf.writeInt(RowType.T_FLOAT_SPARSE.getNumber());
      buf.writeInt(row.size());
      int[] indices = row.getStorage().getIndices();
      float[] values = row.getStorage().getValues();
      for (int i = 0; i < indices.length; i++) {
        buf.writeInt(indices[i]);
        buf.writeFloat(values[i]);
      }
    }
  }

  private void serializeIntIntRow(IntIntVector row, ByteBuf buf) {
    if (row.isDense()) {
      buf.writeInt(RowType.T_INT_DENSE.getNumber());
      int[] values = row.getStorage().getValues();
      buf.writeInt(values.length);
      for (int i = 0; i < values.length; i++) {
        buf.writeInt(values[i]);
      }
    } else if (row.isSparse()) {
      buf.writeInt(RowType.T_FLOAT_SPARSE.getNumber());
      buf.writeInt(row.size());
      ObjectIterator<Int2IntMap.Entry> iter = row.getStorage().entryIterator();
      Int2IntMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeInt(entry.getIntKey());
        buf.writeInt(entry.getIntValue());
      }
    } else {
      buf.writeInt(RowType.T_FLOAT_SPARSE.getNumber());
      buf.writeInt(row.size());
      int[] indices = row.getStorage().getIndices();
      int[] values = row.getStorage().getValues();
      for (int i = 0; i < indices.length; i++) {
        buf.writeInt(indices[i]);
        buf.writeInt(values[i]);
      }
    }
  }

  private void serializeIntLongRow(IntLongVector row, ByteBuf buf) {
    if (row.isDense()) {
      buf.writeInt(RowType.T_LONG_DENSE.getNumber());
      long[] values = row.getStorage().getValues();
      buf.writeInt(values.length);
      for (int i = 0; i < values.length; i++) {
        buf.writeLong(values[i]);
      }
    } else if (row.isSparse()) {
      buf.writeInt(RowType.T_LONG_SPARSE.getNumber());
      buf.writeInt(row.size());
      ObjectIterator<Int2LongMap.Entry> iter = row.getStorage().entryIterator();
      Int2LongMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeInt(entry.getIntKey());
        buf.writeLong(entry.getLongValue());
      }
    } else {
      buf.writeInt(RowType.T_LONG_SPARSE.getNumber());
      buf.writeInt(row.size());
      int[] indices = row.getStorage().getIndices();
      long[] values = row.getStorage().getValues();
      for (int i = 0; i < indices.length; i++) {
        buf.writeInt(indices[i]);
        buf.writeLong(values[i]);
      }
    }
  }

  private void serializeLongDoubleRow(LongDoubleVector row, ByteBuf buf) {
    if (row.isSparse()) {
      buf.writeInt(RowType.T_DOUBLE_SPARSE_LONGKEY.getNumber());
      buf.writeInt((int) row.size());
      ObjectIterator<Long2DoubleMap.Entry> iter = row.getStorage().entryIterator();
      Long2DoubleMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeLong(entry.getLongKey());
        buf.writeDouble(entry.getDoubleValue());
      }
    } else {
      buf.writeInt(RowType.T_DOUBLE_SPARSE_LONGKEY.getNumber());
      buf.writeInt((int) row.size());
      long[] indices = row.getStorage().getIndices();
      double[] values = row.getStorage().getValues();
      for (int i = 0; i < indices.length; i++) {
        buf.writeLong(indices[i]);
        buf.writeDouble(values[i]);
      }
    }
  }

  private void serializeLongFloatRow(LongFloatVector row, ByteBuf buf) {
    if (row.isSparse()) {
      buf.writeInt(RowType.T_FLOAT_SPARSE_LONGKEY.getNumber());
      buf.writeInt((int) row.size());
      ObjectIterator<Long2FloatMap.Entry> iter = row.getStorage().entryIterator();
      Long2FloatMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeLong(entry.getLongKey());
        buf.writeFloat(entry.getFloatValue());
      }
    } else {
      buf.writeInt(RowType.T_FLOAT_SPARSE_LONGKEY.getNumber());
      buf.writeInt((int) row.size());
      long[] indices = row.getStorage().getIndices();
      float[] values = row.getStorage().getValues();
      for (int i = 0; i < indices.length; i++) {
        buf.writeLong(indices[i]);
        buf.writeFloat(values[i]);
      }
    }
  }

  private void serializeLongIntRow(LongIntVector row, ByteBuf buf) {
    if (row.isSparse()) {
      buf.writeInt(RowType.T_INT_SPARSE_LONGKEY.getNumber());
      buf.writeInt((int) row.size());
      ObjectIterator<Long2IntMap.Entry> iter = row.getStorage().entryIterator();
      Long2IntMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeLong(entry.getLongKey());
        buf.writeInt(entry.getIntValue());
      }
    } else {
      buf.writeInt(RowType.T_INT_SPARSE_LONGKEY.getNumber());
      buf.writeInt((int) row.size());
      long[] indices = row.getStorage().getIndices();
      int[] values = row.getStorage().getValues();
      for (int i = 0; i < indices.length; i++) {
        buf.writeLong(indices[i]);
        buf.writeInt(values[i]);
      }
    }
  }

  private void serializeLongLongRow(LongLongVector row, ByteBuf buf) {
    if (row.isSparse()) {
      buf.writeInt(RowType.T_LONG_SPARSE_LONGKEY.getNumber());
      buf.writeInt((int) row.size());
      ObjectIterator<Long2LongMap.Entry> iter = row.getStorage().entryIterator();
      Long2LongMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeLong(entry.getLongKey());
        buf.writeLong(entry.getLongValue());
      }
    } else {
      buf.writeInt(RowType.T_LONG_SPARSE_LONGKEY.getNumber());
      buf.writeInt((int) row.size());
      long[] indices = row.getStorage().getIndices();
      long[] values = row.getStorage().getValues();
      for (int i = 0; i < indices.length; i++) {
        buf.writeLong(indices[i]);
        buf.writeLong(values[i]);
      }
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////////
  private void serializeIntDoubleRow(IntDoubleVector row, ByteBuf buf, PartitionKey partKey,
    boolean needCheck, boolean isComp) {
    int endCol = (int) partKey.getEndCol();
    int startCol = (int) partKey.getStartCol();
    int offset = (int) partKey.getStartCol();
    if (isComp) {
      endCol -= startCol;
      startCol = 0;
      offset = 0;
    }

    if (row.isDense()) {
      buf.writeInt(RowType.T_DOUBLE_DENSE.getNumber());
      double[] values = row.getStorage().getValues();
      buf.writeInt(endCol - startCol);
      for (int i = startCol; i < endCol; i++) {
        buf.writeDouble(values[i]);
      }
    } else if (row.isSparse()) {
      buf.writeInt(RowType.T_DOUBLE_SPARSE.getNumber());
      int pos = buf.writerIndex();
      buf.writeInt(0);
      int num = 0;
      ObjectIterator<Int2DoubleMap.Entry> iter = row.getStorage().entryIterator();
      Int2DoubleMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        if (!needCheck || colInPart(entry.getIntKey(), partKey)) {
          buf.writeInt(entry.getIntKey() - offset);
          buf.writeDouble(entry.getDoubleValue());
          num++;
        }
      }
      buf.setInt(pos, num);
    } else {
      buf.writeInt(RowType.T_DOUBLE_SPARSE.getNumber());
      int pos = buf.writerIndex();
      buf.writeInt(0);
      int num = 0;
      int[] indices = row.getStorage().getIndices();
      double[] values = row.getStorage().getValues();
      for (int i = 0; i < indices.length; i++) {
        if (!needCheck || colInPart(indices[i], partKey)) {
          buf.writeInt(indices[i] - offset);
          buf.writeDouble(values[i]);
          num++;
        }
      }
      buf.setInt(pos, num);
    }
  }

  private boolean colInPart(int key, PartitionKey partKey) {
    return key >= partKey.getStartCol() && key < partKey.getEndCol();
  }

  private boolean colInPart(long key, PartitionKey partKey) {
    return key >= partKey.getStartCol() && key < partKey.getEndCol();
  }

  private void serializeIntFloatRow(IntFloatVector row, ByteBuf buf, PartitionKey partKey,
    boolean needCheck, boolean isComp) {
    int endCol = (int) partKey.getEndCol();
    int startCol = (int) partKey.getStartCol();
    int offset = (int) partKey.getStartCol();
    if (isComp) {
      endCol -= startCol;
      startCol = 0;
      offset = 0;
    }

    if (row.isDense()) {
      buf.writeInt(RowType.T_FLOAT_DENSE.getNumber());
      float[] values = row.getStorage().getValues();
      buf.writeInt(endCol - startCol);
      for (int i = startCol; i < endCol; i++) {
        buf.writeFloat(values[i]);
      }
    } else if (row.isSparse()) {
      buf.writeInt(RowType.T_FLOAT_SPARSE.getNumber());
      int pos = buf.writerIndex();
      buf.writeInt(0);
      int num = 0;
      ObjectIterator<Int2FloatMap.Entry> iter = row.getStorage().entryIterator();
      Int2FloatMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        if (!needCheck || colInPart(entry.getIntKey(), partKey)) {
          buf.writeInt(entry.getIntKey() - offset);
          buf.writeFloat(entry.getFloatValue());
          num++;
        }
      }
      buf.setInt(pos, num);
    } else {
      buf.writeInt(RowType.T_FLOAT_SPARSE.getNumber());
      int pos = buf.writerIndex();
      buf.writeInt(0);
      int num = 0;
      int[] indices = row.getStorage().getIndices();
      float[] values = row.getStorage().getValues();
      for (int i = 0; i < indices.length; i++) {
        if (!needCheck || colInPart(indices[i], partKey)) {
          buf.writeInt(indices[i] - offset);
          buf.writeFloat(values[i]);
          num++;
        }
      }
      buf.setInt(pos, num);
    }
  }

  private void serializeIntIntRow(IntIntVector row, ByteBuf buf, PartitionKey partKey,
    boolean needCheck, boolean isComp) {
    int endCol = (int) partKey.getEndCol();
    int startCol = (int) partKey.getStartCol();
    int offset = (int) partKey.getStartCol();
    if (isComp) {
      endCol -= startCol;
      startCol = 0;
      offset = 0;
    }

    if (row.isDense()) {
      buf.writeInt(RowType.T_INT_DENSE.getNumber());
      int[] values = row.getStorage().getValues();
      buf.writeInt(endCol - startCol);
      for (int i = startCol; i < endCol; i++) {
        buf.writeInt(values[i]);
      }
    } else if (row.isSparse()) {
      buf.writeInt(RowType.T_INT_SPARSE.getNumber());
      int pos = buf.writerIndex();
      buf.writeInt(0);
      int num = 0;
      ObjectIterator<Int2IntMap.Entry> iter = row.getStorage().entryIterator();
      Int2IntMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        if (!needCheck || colInPart(entry.getIntKey(), partKey)) {
          buf.writeInt(entry.getIntKey() - offset);
          buf.writeInt(entry.getIntValue());
          num++;
        }
      }
      buf.setInt(pos, num);
    } else {
      buf.writeInt(RowType.T_INT_SPARSE.getNumber());
      int pos = buf.writerIndex();
      buf.writeInt(0);
      int num = 0;
      int[] indices = row.getStorage().getIndices();
      int[] values = row.getStorage().getValues();
      for (int i = 0; i < indices.length; i++) {
        if (!needCheck || colInPart(indices[i], partKey)) {
          buf.writeInt(indices[i] - offset);
          buf.writeInt(values[i]);
          num++;
        }
      }
      buf.setInt(pos, num);
    }
  }

  private void serializeIntLongRow(IntLongVector row, ByteBuf buf, PartitionKey partKey,
    boolean needCheck, boolean isComp) {
    int endCol = (int) partKey.getEndCol();
    int startCol = (int) partKey.getStartCol();
    int offset = (int) partKey.getStartCol();
    if (isComp) {
      endCol -= startCol;
      startCol = 0;
      offset = 0;
    }

    if (row.isDense()) {
      buf.writeInt(RowType.T_LONG_DENSE.getNumber());
      long[] values = row.getStorage().getValues();
      buf.writeInt(endCol - startCol);
      for (int i = startCol; i < endCol; i++) {
        buf.writeLong(values[i]);
      }
    } else if (row.isSparse()) {
      buf.writeInt(RowType.T_LONG_SPARSE.getNumber());
      int pos = buf.writerIndex();
      buf.writeInt(0);
      int num = 0;
      ObjectIterator<Int2LongMap.Entry> iter = row.getStorage().entryIterator();
      Int2LongMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        if (!needCheck || colInPart(entry.getIntKey(), partKey)) {
          buf.writeInt(entry.getIntKey() - offset);
          buf.writeLong(entry.getLongValue());
          num++;
        }
      }
      buf.setInt(pos, num);
    } else {
      buf.writeInt(RowType.T_LONG_SPARSE.getNumber());
      int pos = buf.writerIndex();
      buf.writeInt(0);
      int num = 0;
      int[] indices = row.getStorage().getIndices();
      long[] values = row.getStorage().getValues();
      for (int i = 0; i < indices.length; i++) {
        if (!needCheck || colInPart(indices[i], partKey)) {
          buf.writeInt(indices[i] - offset);
          buf.writeLong(values[i]);
          num++;
        }
      }
      buf.setInt(pos, num);
    }
  }

  private void serializeLongDoubleRow(LongDoubleVector row, ByteBuf buf, PartitionKey partKey,
    boolean needCheck, boolean isComp) {
    int endCol = (int) partKey.getEndCol();
    int startCol = (int) partKey.getStartCol();
    long offset = partKey.getStartCol();
    if (isComp) {
      endCol -= startCol;
      startCol = 0;
      offset = 0;
    }

    if (row.isSparse()) {
      buf.writeInt(RowType.T_DOUBLE_SPARSE_LONGKEY.getNumber());

      // TODO
      buf.writeDouble(0);
      int pos = buf.writerIndex();
      buf.writeInt(0);
      int num = 0;
      ObjectIterator<Long2DoubleMap.Entry> iter = row.getStorage().entryIterator();
      Long2DoubleMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        if (!needCheck || colInPart(entry.getLongKey(), partKey)) {
          buf.writeLong(entry.getLongKey() - offset);
          buf.writeDouble(entry.getDoubleValue());
          num++;
        }
      }
      buf.setInt(pos, num);
    } else {
      buf.writeInt(RowType.T_DOUBLE_SPARSE_LONGKEY.getNumber());

      // TODO
      buf.writeDouble(0);
      int pos = buf.writerIndex();
      buf.writeInt(0);
      int num = 0;
      long[] indices = row.getStorage().getIndices();
      double[] values = row.getStorage().getValues();
      for (int i = 0; i < indices.length; i++) {
        if (!needCheck || colInPart(indices[i], partKey)) {
          buf.writeLong(indices[i] - offset);
          buf.writeDouble(values[i]);
          num++;
        }
      }
      buf.setInt(pos, num);
    }
  }

  private void serializeLongFloatRow(LongFloatVector row, ByteBuf buf, PartitionKey partKey,
    boolean needCheck, boolean isComp) {
    int endCol = (int) partKey.getEndCol();
    int startCol = (int) partKey.getStartCol();
    long offset = partKey.getStartCol();
    if (isComp) {
      endCol -= startCol;
      startCol = 0;
      offset = 0;
    }

    if (row.isSparse()) {
      buf.writeInt(RowType.T_FLOAT_SPARSE_LONGKEY.getNumber());
      int pos = buf.writerIndex();
      buf.writeInt(0);
      int num = 0;
      ObjectIterator<Long2FloatMap.Entry> iter = row.getStorage().entryIterator();
      Long2FloatMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        if (!needCheck || colInPart(entry.getLongKey(), partKey)) {
          buf.writeLong(entry.getLongKey() - offset);
          buf.writeFloat(entry.getFloatValue());
          num++;
        }
      }
      buf.setInt(pos, num);
    } else {
      buf.writeInt(RowType.T_FLOAT_SPARSE_LONGKEY.getNumber());
      int pos = buf.writerIndex();
      buf.writeInt(0);
      int num = 0;
      long[] indices = row.getStorage().getIndices();
      float[] values = row.getStorage().getValues();
      for (int i = 0; i < indices.length; i++) {
        if (!needCheck || colInPart(indices[i], partKey)) {
          buf.writeLong(indices[i] - offset);
          buf.writeFloat(values[i]);
          num++;
        }
      }

      buf.setInt(pos, num);
    }
  }

  private void serializeLongIntRow(LongIntVector row, ByteBuf buf, PartitionKey partKey,
    boolean needCheck, boolean isComp) {
    int endCol = (int) partKey.getEndCol();
    int startCol = (int) partKey.getStartCol();
    long offset = partKey.getStartCol();
    if (isComp) {
      endCol -= startCol;
      startCol = 0;
      offset = 0;
    }

    if (row.isSparse()) {
      buf.writeInt(RowType.T_INT_SPARSE_LONGKEY.getNumber());
      int pos = buf.writerIndex();
      buf.writeInt(0);
      int num = 0;
      ObjectIterator<Long2IntMap.Entry> iter = row.getStorage().entryIterator();
      Long2IntMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        if (!needCheck || colInPart(entry.getLongKey(), partKey)) {
          buf.writeLong(entry.getLongKey() - offset);
          buf.writeInt(entry.getIntValue());
          num++;
        }
      }
      buf.setInt(pos, num);
    } else {
      buf.writeInt(RowType.T_INT_SPARSE_LONGKEY.getNumber());
      int pos = buf.writerIndex();
      buf.writeInt(0);
      int num = 0;
      long[] indices = row.getStorage().getIndices();
      int[] values = row.getStorage().getValues();
      for (int i = 0; i < indices.length; i++) {
        if (!needCheck || colInPart(indices[i], partKey)) {
          buf.writeLong(indices[i] - offset);
          buf.writeInt(values[i]);
          num++;
        }
      }
      buf.setInt(pos, num);
    }
  }

  private void serializeLongLongRow(LongLongVector row, ByteBuf buf, PartitionKey partKey,
    boolean needCheck, boolean isComp) {
    int endCol = (int) partKey.getEndCol();
    int startCol = (int) partKey.getStartCol();
    long offset = partKey.getStartCol();
    if (isComp) {
      endCol -= startCol;
      startCol = 0;
      offset = 0;
    }

    if (row.isSparse()) {
      buf.writeInt(RowType.T_LONG_SPARSE_LONGKEY.getNumber());
      int pos = buf.writerIndex();
      buf.writeInt(0);
      int num = 0;
      ObjectIterator<Long2LongMap.Entry> iter = row.getStorage().entryIterator();
      Long2LongMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        if (!needCheck || colInPart(entry.getLongKey(), partKey)) {
          buf.writeLong(entry.getLongKey() - offset);
          buf.writeLong(entry.getLongValue());
          num++;
        }
      }
      buf.setInt(pos, num);
    } else {
      buf.writeInt(RowType.T_LONG_SPARSE_LONGKEY.getNumber());
      int pos = buf.writerIndex();
      buf.writeInt(0);
      int num = 0;
      long[] indices = row.getStorage().getIndices();
      long[] values = row.getStorage().getValues();
      for (int i = 0; i < indices.length; i++) {
        if (!needCheck || colInPart(indices[i], partKey)) {
          buf.writeLong(indices[i] - offset);
          buf.writeLong(values[i]);
          num++;
        }
      }
      buf.setInt(pos, num);
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  private void serializeIntDoubleCompRow(CompIntDoubleVector row, ByteBuf buf, PartitionKey partKey,
    boolean needCheck) {
    List<PartitionKey> parts =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(row.getMatrixId(), row.getRowId());
    IntDoubleVector[] partVecs = row.getPartitions();
    assert parts.size() == partVecs.length;
    for (int i = 0; i < partVecs.length; i++) {
      if (parts.get(i).getPartitionId() == partKey.getPartitionId()) {
        serializeIntDoubleRow(partVecs[i], buf, partKey, needCheck, true);
        return;
      }
    }
    return;
  }

  private void serializeIntFloatCompRow(CompIntFloatVector row, ByteBuf buf, PartitionKey partKey,
    boolean needCheck) {
    List<PartitionKey> parts =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(row.getMatrixId(), row.getRowId());
    IntFloatVector[] partVecs = row.getPartitions();
    assert parts.size() == partVecs.length;
    for (int i = 0; i < partVecs.length; i++) {
      if (parts.get(i).getPartitionId() == partKey.getPartitionId()) {
        serializeIntFloatRow(partVecs[i], buf, partKey, needCheck, true);
        return;
      }
    }
    return;
  }

  private void serializeIntIntCompRow(CompIntIntVector row, ByteBuf buf, PartitionKey partKey,
    boolean needCheck) {
    List<PartitionKey> parts =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(row.getMatrixId(), row.getRowId());
    IntIntVector[] partVecs = row.getPartitions();
    assert parts.size() == partVecs.length;
    for (int i = 0; i < partVecs.length; i++) {
      if (parts.get(i).getPartitionId() == partKey.getPartitionId()) {
        serializeIntIntRow(partVecs[i], buf, partKey, needCheck, true);
        return;
      }
    }
    return;
  }

  private void serializeIntLongCompRow(CompIntLongVector row, ByteBuf buf, PartitionKey partKey,
    boolean needCheck) {
    List<PartitionKey> parts =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(row.getMatrixId(), row.getRowId());
    IntLongVector[] partVecs = row.getPartitions();
    assert parts.size() == partVecs.length;
    for (int i = 0; i < partVecs.length; i++) {
      if (parts.get(i).getPartitionId() == partKey.getPartitionId()) {
        serializeIntLongRow(partVecs[i], buf, partKey, needCheck, true);
        return;
      }
    }
    return;
  }

  private void serializeLongDoubleCompRow(CompLongDoubleVector row, ByteBuf buf,
    PartitionKey partKey, boolean needCheck) {
    List<PartitionKey> parts =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(row.getMatrixId(), row.getRowId());
    LongDoubleVector[] partVecs = row.getPartitions();
    assert parts.size() == partVecs.length;
    for (int i = 0; i < partVecs.length; i++) {
      if (parts.get(i).getPartitionId() == partKey.getPartitionId()) {
        serializeLongDoubleRow(partVecs[i], buf, partKey, needCheck, true);
        return;
      }
    }
    return;
  }

  private void serializeLongFloatCompRow(CompLongFloatVector row, ByteBuf buf, PartitionKey partKey,
    boolean needCheck) {
    List<PartitionKey> parts =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(row.getMatrixId(), row.getRowId());
    LongFloatVector[] partVecs = row.getPartitions();
    assert parts.size() == partVecs.length;
    for (int i = 0; i < partVecs.length; i++) {
      if (parts.get(i).getPartitionId() == partKey.getPartitionId()) {
        serializeLongFloatRow(partVecs[i], buf, partKey, needCheck, true);
        return;
      }
    }
    return;
  }

  private void serializeLongIntCompRow(CompLongIntVector row, ByteBuf buf, PartitionKey partKey,
    boolean needCheck) {
    List<PartitionKey> parts =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(row.getMatrixId(), row.getRowId());
    LongIntVector[] partVecs = row.getPartitions();
    assert parts.size() == partVecs.length;
    for (int i = 0; i < partVecs.length; i++) {
      if (parts.get(i).getPartitionId() == partKey.getPartitionId()) {
        serializeLongIntRow(partVecs[i], buf, partKey, needCheck, true);
        return;
      }
    }
    return;
  }

  private void serializeLongLongCompRow(CompLongLongVector row, ByteBuf buf, PartitionKey partKey,
    boolean needCheck) {
    List<PartitionKey> parts =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(row.getMatrixId(), row.getRowId());
    LongLongVector[] partVecs = row.getPartitions();
    assert parts.size() == partVecs.length;
    for (int i = 0; i < partVecs.length; i++) {
      if (parts.get(i).getPartitionId() == partKey.getPartitionId()) {
        serializeLongLongRow(partVecs[i], buf, partKey, needCheck, true);
        return;
      }
    }
    return;
  }

  @Override public int size() {
    return 0;
  }
}
