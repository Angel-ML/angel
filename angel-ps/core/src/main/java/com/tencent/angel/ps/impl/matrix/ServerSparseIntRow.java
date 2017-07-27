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

import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.protobuf.generated.MLProtos.RowType;
import io.netty.buffer.ByteBuf;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The class represent sparse int row on parameter server.
 */
public class ServerSparseIntRow extends ServerRow {

  private Int2IntOpenHashMap hashMap;

  public ServerSparseIntRow(int rowId, int startCol, int endCol) {
    super(rowId, startCol, endCol);
    this.hashMap = new Int2IntOpenHashMap();
  }

  public ServerSparseIntRow() {
    this(0, 0, 0);
  }

  @Override
  public MLProtos.RowType getRowType() {
    return MLProtos.RowType.T_INT_SPARSE;
  }

  @Override
  public void writeTo(DataOutputStream output) throws IOException {
    try {
      lock.readLock().lock();
      super.writeTo(output);
      output.writeInt(hashMap.size());
      for (Int2IntMap.Entry entry : hashMap.int2IntEntrySet()) {
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
        hashMap.addTo(input.readInt(), input.readInt());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public int size() {
    return hashMap.size();
  }

  @Override
  public void update(RowType rowType, ByteBuf buf, int size) {
    try {
      lock.writeLock().lock();
      switch (rowType) {
        case T_INT_SPARSE:
          updateIntSparse(buf, size);
          break;

        case T_INT_DENSE:
          updateIntDense(buf, size);
          break;
        default:
          break;
      }

      updateRowVersion();
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void updateIntDense(ByteBuf buf, int size) {
    for (int i = 0; i < size; i++) {
      hashMap.addTo(i, buf.readInt());
    }
  }

  private void updateIntSparse(ByteBuf buf, int size) {
    ByteBuf valueBuf = buf.slice(buf.readerIndex() + size * 4, size * 4);
    for (int i = 0; i < size; i++) {
      hashMap.addTo(buf.readInt(), valueBuf.readInt());
    }
    buf.readerIndex(buf.readerIndex() + size * 4);
  }

  public Int2IntOpenHashMap getData() {
    return hashMap;
  }

  @Override
  public void serialize(ByteBuf buf) {
    try {
      lock.readLock().lock();
      super.serialize(buf);
      buf.writeInt(hashMap.size());
      for (Int2IntMap.Entry entry : hashMap.int2IntEntrySet()) {
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
      if (hashMap == null) {
        hashMap = new Int2IntOpenHashMap(elemNum);
      }
      for (int i = 0; i < elemNum; i++) {
        hashMap.put(buf.readInt(), buf.readInt());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public int bufferLen() {
    try {
      lock.readLock().lock();
      return super.bufferLen() + 4 + hashMap.size() * 8;
    } finally {
      lock.readLock().unlock();
    }
  }

  public void mergeIntDense(int size, ByteBuf buf) {
    try {
      lock.writeLock().lock();
      for (int i = 0; i < size; i++) {
        hashMap.addTo(i, buf.readInt());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void mergeIntSparse(int size, ByteBuf buf) {
    try {
      lock.writeLock().lock();
      ByteBuf valueBuf = buf.slice(buf.readerIndex() + size * 4, size * 4);
      for (int i = 0; i < size; i++) {
        hashMap.addTo(buf.readInt(), valueBuf.readInt());
      }
      buf.readerIndex(buf.readerIndex() + size * 4);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void mergeTo(Int2IntOpenHashMap indexToValueMap) {
    try {
      lock.readLock().lock();
      indexToValueMap.putAll(hashMap);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void mergeTo(int[] indexes, int[] values, int startPos, int len) {
    try {
      lock.readLock().lock();
      int writeLen = len < hashMap.size() ? len : hashMap.size();
      if (writeLen == 0) {
        return;
      }

      int index = 0;
      for (Int2IntMap.Entry entry : hashMap.int2IntEntrySet()) {
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
}
