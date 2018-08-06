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
 */

package com.tencent.angel.psagent.matrix.transport.adapter;

import com.tencent.angel.common.Serialize;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.*;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2FloatMap;
import it.unimi.dsi.fastutil.longs.Long2FloatOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class RowsUpdateSplit implements Serialize {
  private final TVector [] rows;
  private final int startPos;
  private final int endPos;

  public RowsUpdateSplit(TVector [] rows, int startPos, int endPos) {
    this.rows = rows;
    this.startPos = startPos;
    this.endPos = endPos;
  }

  public RowsUpdateSplit() {
    this(null, 0, 0);
  }

  public TVector [] getRows() {
    return rows;
  }

  public int getStartPos() {
    return startPos;
  }

  public int getEndPos() {
    return endPos;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(endPos - startPos);
    for(int i = startPos; i < endPos; i++) {
      serializeRow(buf, rows[i]);
    }
  }

  private void serializeRow(ByteBuf buf, TVector row) {
    buf.writeInt(row.getRowId());
    buf.writeInt(row.getType().getNumber());
    switch (row.getType()) {
      case T_DOUBLE_DENSE: {
        double [] values = ((DenseDoubleVector) row).getValues();
        buf.writeInt(values.length);
        for(int i = 0; i < values.length; i++) {
          buf.writeDouble(values[i]);
        }
        break;
      }

      case T_DOUBLE_SPARSE: {
        Int2DoubleOpenHashMap data = ((SparseDoubleVector) row).getIndexToValueMap();
        buf.writeInt(data.size());
        ObjectIterator<Int2DoubleMap.Entry> iter = data.int2DoubleEntrySet().fastIterator();
        Int2DoubleMap.Entry entry;
        while(iter.hasNext()) {
          entry = iter.next();
          buf.writeInt(entry.getIntKey());
          buf.writeDouble(entry.getDoubleValue());
        }
        break;
      }

      case T_DOUBLE_SPARSE_LONGKEY: {
        Long2DoubleOpenHashMap data = ((SparseLongKeyDoubleVector) row).getIndexToValueMap();
        buf.writeInt(data.size());
        ObjectIterator<Long2DoubleMap.Entry> iter = data.long2DoubleEntrySet().fastIterator();
        Long2DoubleMap.Entry entry;
        while(iter.hasNext()) {
          entry = iter.next();
          buf.writeLong(entry.getLongKey());
          buf.writeDouble(entry.getDoubleValue());
        }
        break;
      }

      case T_FLOAT_DENSE: {
        float [] values = ((DenseFloatVector) row).getValues();
        buf.writeInt(values.length);
        for(int i = 0; i < values.length; i++) {
          buf.writeFloat(values[i]);
        }
        break;
      }

      case T_FLOAT_SPARSE: {
        Int2FloatOpenHashMap data = ((SparseFloatVector) row).getIndexToValueMap();
        buf.writeInt(data.size());
        ObjectIterator<Int2FloatMap.Entry> iter = data.int2FloatEntrySet().fastIterator();
        Int2FloatMap.Entry entry;
        while(iter.hasNext()) {
          entry = iter.next();
          buf.writeInt(entry.getIntKey());
          buf.writeFloat(entry.getFloatValue());
        }
        break;
      }

      case T_FLOAT_SPARSE_LONGKEY: {
        Long2FloatOpenHashMap data = ((SparseLongKeyFloatVector) row).getIndexToValueMap();
        buf.writeInt(data.size());
        ObjectIterator<Long2FloatMap.Entry> iter = data.long2FloatEntrySet().fastIterator();
        Long2FloatMap.Entry entry;
        while(iter.hasNext()) {
          entry = iter.next();
          buf.writeLong(entry.getLongKey());
          buf.writeFloat(entry.getFloatValue());
        }
        break;
      }

      case T_INT_DENSE: {
        int [] values = ((DenseIntVector) row).getValues();
        buf.writeInt(values.length);
        for(int i = 0; i < values.length; i++) {
          buf.writeInt(values[i]);
        }
        break;
      }

      case T_INT_SPARSE: {
        Int2IntOpenHashMap data = ((SparseIntVector) row).getIndexToValueMap();
        buf.writeInt(data.size());
        ObjectIterator<Int2IntMap.Entry> iter = data.int2IntEntrySet().fastIterator();
        Int2IntMap.Entry entry;
        while(iter.hasNext()) {
          entry = iter.next();
          buf.writeInt(entry.getIntKey());
          buf.writeInt(entry.getIntValue());
        }
        break;
      }

      default:
        throw new UnsupportedOperationException("Unsupport row type " + row.getType());
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    //unused now
  }

  @Override
  public int bufferLen() {
    int len = 4;
    for(int i = startPos; i < endPos; i++) {
      len += rowBufferLen(rows[i]);
    }
    return len;
  }

  private int rowBufferLen(TVector row) {
    int len = 12;
    switch (row.getType()) {
      case T_DOUBLE_DENSE:
        len += 8 * row.size();
        break;

      case T_DOUBLE_SPARSE:
        len += 12 * row.size();
        break;

      case T_FLOAT_DENSE:
      case T_INT_DENSE:
        len += 4 * row.size();
        break;

      case T_FLOAT_SPARSE:
      case T_INT_SPARSE:
        len += 8 * row.size();
        break;

      case T_DOUBLE_SPARSE_LONGKEY:
        len += 16 * row.size();
        break;

      case T_FLOAT_SPARSE_LONGKEY:
        len += 12 * row.size();
        break;

      default:
        throw new UnsupportedOperationException("Unsupport row type " + row.getType());
    }

    return len;
  }

  public int size() {
    return endPos - startPos;
  }
}
