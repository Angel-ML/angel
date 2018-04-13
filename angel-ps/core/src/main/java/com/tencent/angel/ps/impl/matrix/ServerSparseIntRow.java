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
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The class represent sparse int row on parameter server.
 */
public class ServerSparseIntRow extends ServerIntRow {

  /** Index->value map */
  private Int2IntOpenHashMap data;

  /**
   * Create a ServerSparseIntRow
   * @param rowId row index
   * @param startCol partition start column index
   * @param endCol partition end column index
   */
  public ServerSparseIntRow(int rowId, int startCol, int endCol, int estEleNum) {
    super(rowId, startCol, endCol);
    this.data = new Int2IntOpenHashMap(estEleNum);
  }

  /**
   * Create a ServerSparseIntRow
   */
  public ServerSparseIntRow() {
    this(0, 0, 0, 0);
  }

  @Override
  public RowType getRowType() {
    return RowType.T_INT_SPARSE;
  }

  @Override
  public void writeTo(DataOutputStream output) throws IOException {
    try {
      lock.readLock().lock();
      super.writeTo(output);
      output.writeInt(data.size());

      ObjectIterator<Int2IntMap.Entry> iter = data.int2IntEntrySet().fastIterator();
      Int2IntMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        output.writeInt(entry.getIntKey());
        output.writeInt(entry.getIntValue());
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void readFrom(DataInputStream input) throws IOException {
    try {
      lock.writeLock().lock();
      super.readFrom(input);
      int nnz = input.readInt();
      for (int i = 0; i < nnz; i++) {
        data.addTo(input.readInt(), input.readInt());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public int size() {
    try {
      lock.readLock().lock();
      return data.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void update(RowType rowType, ByteBuf buf) {
    tryToLockWrite();
    try {
      switch (rowType) {
        case T_INT_SPARSE:
        case T_INT_SPARSE_COMPONENT:
          updateIntSparse(buf);
          break;

        case T_INT_DENSE:
          updateIntDense(buf);
          break;

        default:
          throw new UnsupportedOperationException("Unsupport operation: update " + rowType + " to " + this.getClass().getName());
      }

      updateRowVersion();
    } finally {
      unlockWrite();
    }
  }

  private void resizeHashMap(int size) {
    if(data.size() < size) {
      Int2IntOpenHashMap oldMap = data;
      data = new Int2IntOpenHashMap(size);
      data.putAll(oldMap);
    }
  }

  private void updateIntDense(ByteBuf buf) {
    int size = buf.readInt();
    resizeHashMap(size);
    for (int i = 0; i < size; i++) {
      data.addTo(i, buf.readInt());
    }
  }

  private void updateIntSparse(ByteBuf buf) {
    int size = buf.readInt();
    resizeHashMap(size);
    for (int i = 0; i < size; i++) {
      data.addTo(buf.readInt(), buf.readInt());
    }
  }

  public Int2IntOpenHashMap getData() {
    return data;
  }

  @Override
  public void serialize(ByteBuf buf) {
    try {
      lock.readLock().lock();
      super.serialize(buf);
      buf.writeInt(data.size());

      ObjectIterator<Int2IntMap.Entry> iter = data.int2IntEntrySet().fastIterator();
      Int2IntMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeInt(entry.getIntKey());
        buf.writeInt(entry.getIntValue());
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    try {
      lock.writeLock().lock();
      super.deserialize(buf);
      int elemNum = buf.readInt();
      if (data == null) {
        data = new Int2IntOpenHashMap(elemNum);
      }
      for (int i = 0; i < elemNum; i++) {
        data.put(buf.readInt(), buf.readInt());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public int bufferLen() {
    try {
      lock.readLock().lock();
      return super.bufferLen() + 4 + data.size() * 8;
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

  public void mergeIntDense(int size, ByteBuf buf) {
    try {
      lock.writeLock().lock();
      for (int i = 0; i < size; i++) {
        data.addTo(i, buf.readInt());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Merge a serialized dense int vector split to this sparse float int split
   * @param size dense int vector split length
   * @param buf serialized dense int vector split
   */
  public void mergeIntSparse(int size, ByteBuf buf) {
    try {
      lock.writeLock().lock();
      for (int i = 0; i < size; i++) {
        data.addTo(buf.readInt(), buf.readInt());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Merge this sparse int vector split to a map
   * @param indexToValueMap a index->value map
   */
  public void mergeTo(Int2IntOpenHashMap indexToValueMap) {
    try {
      lock.readLock().lock();
      indexToValueMap.putAll(data);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Merge this sparse int vector split to a index/value array
   * @param indexes index array
   * @param values value array
   * @param startPos write start position of the index/value array
   * @param len write length
   */
  public void mergeTo(int[] indexes, int[] values, int startPos, int len) {
    try {
      lock.readLock().lock();
      int writeLen = len < data.size() ? len : data.size();
      if (writeLen == 0) {
        return;
      }

      int index = 0;
      ObjectIterator<Int2IntMap.Entry> iter = data.int2IntEntrySet().fastIterator();
      Int2IntMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        indexes[startPos + index] = entry.getIntKey();
        values[startPos + index] = entry.getIntValue();
        index++;
        if (index == writeLen) {
          return;
        }
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override protected int getValue(int index) {
    return data.get(index);
  }
}
