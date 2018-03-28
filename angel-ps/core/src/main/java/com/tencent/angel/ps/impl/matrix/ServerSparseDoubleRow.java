/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ps.impl.matrix;

import com.tencent.angel.ml.matrix.RowType;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The class represent sparse double row on parameter server.
 */
public class ServerSparseDoubleRow extends ServerDoubleRow {
  private final static Log LOG = LogFactory.getLog(ServerSparseDoubleRow.class);

  /** Index->Value map */
  private Int2DoubleOpenHashMap data;

  /**
   * Create a ServerSparseDoubleRow
   * @param rowId row index
   * @param startCol partition start column index
   * @param endCol partition end column index
   */
  public ServerSparseDoubleRow(int rowId, int startCol, int endCol) {
    super(rowId, startCol, endCol);
    data = new Int2DoubleOpenHashMap();
  }

  /**
   * Create a ServerSparseDoubleRow
   */
  public ServerSparseDoubleRow() {
    this(0, 0, 0);
  }

  @Override public void writeTo(DataOutputStream output) throws IOException {
    try {
      lock.readLock().lock();
      super.writeTo(output);
      output.writeInt(data.size());

      ObjectIterator<Int2DoubleMap.Entry> iter = data.int2DoubleEntrySet().fastIterator();
      Int2DoubleMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        output.writeInt(entry.getIntKey());
        output.writeDouble(entry.getDoubleValue());
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override public void readFrom(DataInputStream input) throws IOException {
    try {
      lock.writeLock().lock();
      super.readFrom(input);
      int nnz = input.readInt();
      for (int i = 0; i < nnz; i++) {
        data.addTo(input.readInt(), input.readDouble());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override public RowType getRowType() {
    return RowType.T_DOUBLE_SPARSE;
  }

  @Override public int size() {
    try {
      lock.readLock().lock();
      return data.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override public void update(RowType rowType, ByteBuf buf) {
    tryToLockWrite();

    try {
      switch (rowType) {
        case T_DOUBLE_SPARSE:
        case T_DOUBLE_SPARSE_COMPONENT:
          updateDoubleSparse(buf);
          break;

        case T_DOUBLE_DENSE:
          updateDoubleDense(buf);
          break;

        default:{
          throw new UnsupportedOperationException("Unsupport operation: update " + rowType + " to " + this.getClass().getName());
        }
      }

      updateRowVersion();
    } finally {
      unlockWrite();
    }
  }

  private void updateDoubleDense(ByteBuf buf) {
    int size = buf.readInt();
    int startColInt = (int) startCol;
    resizeHashMap(size);
    for (int i = 0; i < size; i++) {
      data.addTo(i + startColInt, buf.readDouble());
    }
  }

  private void updateDoubleSparse(ByteBuf buf) {
    int size = buf.readInt();
    resizeHashMap(size);
    for (int i = 0; i < size; i++) {
      data.addTo(buf.readInt(), buf.readDouble());
    }
  }

  private void resizeHashMap(int size) {
    if(data.size() < size) {
      Int2DoubleOpenHashMap oldMap = data;
      data = new Int2DoubleOpenHashMap(size);
      data.putAll(oldMap);
    }
  }

  public Int2DoubleOpenHashMap getData() {
    return data;
  }

  @Override public void serialize(ByteBuf buf) {
    try {
      lock.readLock().lock();
      super.serialize(buf);
      buf.writeInt(data.size());
      ObjectIterator<Int2DoubleMap.Entry> iter = data.int2DoubleEntrySet().fastIterator();

      Int2DoubleMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeInt(entry.getIntKey());
        buf.writeDouble(entry.getDoubleValue());
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override public void deserialize(ByteBuf buf) {
    try {
      lock.writeLock().lock();
      super.deserialize(buf);
      int elemNum = buf.readInt();
      if (data == null) {
        data = new Int2DoubleOpenHashMap(elemNum);
      }
      for (int i = 0; i < elemNum; i++) {
        data.put(buf.readInt(), buf.readDouble());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override public int bufferLen() {
    try {
      lock.readLock().lock();
      return super.bufferLen() + 4 + data.size() * 12;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override public void reset() {
    try {
      lock.writeLock().lock();
      data.clear();
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Merge a serialized dense double vector split to this sparse double vector split
   * @param size dense double vector split length
   * @param buf serialized dense double vector split
   */
  public void mergeDoubleDense(int size, ByteBuf buf) {
    try {
      lock.writeLock().lock();
      resizeHashMap(size);
      int startColInt = (int) startCol;
      for (int i = 0; i < size; i++) {
        data.addTo(i + startColInt, buf.readDouble());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Merge a serialized sparse double vector split to this sparse double vector split
   * @param size sparse double vector split length
   * @param buf serialized dense double vector split
   */
  public void mergeDoubleSparse(int size, ByteBuf buf) {
    try {
      lock.writeLock().lock();
      resizeHashMap(size);
      for (int i = 0; i < size; i++) {
        data.addTo(buf.readInt(), buf.readDouble());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Merge this sparse double vector split to a map
   * @param indexToValueMap a index->value map
   */
  public void mergeTo(Int2DoubleOpenHashMap indexToValueMap) {
    try {
      lock.readLock().lock();
      indexToValueMap.putAll(data);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Merge this sparse double vector split to a index/value array
   * @param indexes index array
   * @param values value array
   * @param startPos write start position of the index/value array
   * @param len write length
   */
  public void mergeTo(int[] indexes, double[] values, int startPos, int len) {
    try {
      lock.readLock().lock();
      int writeLen = len < data.size() ? len : data.size();
      if (writeLen == 0) {
        return;
      }

      int index = 0;
      ObjectIterator<Int2DoubleMap.Entry> iter = data.int2DoubleEntrySet().fastIterator();

      Int2DoubleMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        indexes[startPos + index] = entry.getIntKey();
        values[startPos + index] = entry.getDoubleValue();
        index++;
        if (index == writeLen) {
          return;
        }
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override protected double getValue(int index) {
    return data.get(index);
  }
}
