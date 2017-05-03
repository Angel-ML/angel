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

import com.tencent.angel.protobuf.generated.MLProtos.RowType;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The class represent sparse double row on parameter server.
 */
public class ServerSparseDoubleRow extends ServerRow {
  private final static Log LOG = LogFactory.getLog(ServerSparseDoubleRow.class);

  private Int2DoubleOpenHashMap hashMap;

  public ServerSparseDoubleRow(int rowId, int startCol, int endCol) {
    super(rowId, startCol, endCol);
    hashMap = new Int2DoubleOpenHashMap();
  }

  public ServerSparseDoubleRow() {
    this(0, 0, 0);
  }

  @Override
  public void writeTo(DataOutputStream output) throws IOException {
    try {
      lock.readLock().lock();
      super.writeTo(output);
      output.writeInt(hashMap.size());
      for (Int2DoubleMap.Entry entry : hashMap.int2DoubleEntrySet()) {
        output.writeInt(entry.getIntKey());
        output.writeDouble(entry.getDoubleValue());
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
        hashMap.addTo(input.readInt(), input.readDouble());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public RowType getRowType() {
    return RowType.T_DOUBLE_SPARSE;
  }

  @Override
  public int size() {
    try {
      lock.readLock().lock();
      return hashMap.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void update(RowType rowType, ByteBuf buf, int size) {
    try {
      lock.writeLock().lock();
      switch (rowType) {
        case T_DOUBLE_SPARSE:
          updateDoubleSparse(buf, size);
          break;
        case T_DOUBLE_DENSE:
          updateDoubleDense(buf, size);
          break;
        default:
          LOG.error("invalid rowType: " + rowType.name() + " for ServerSparseDoubleRow!");
      }
      
      updateRowVersion();
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void updateDoubleDense(ByteBuf buf, int size) {
    for (int i = 0; i < size; i++) {
      hashMap.addTo(i, buf.readDouble());
    }
  }

  private void updateDoubleSparse(ByteBuf buf, int size) {
    ByteBuf valueBuf = buf.slice(buf.readerIndex() + size * 4, size * 8);
    for (int i = 0; i < size; i++) {
      hashMap.addTo(buf.readInt(), valueBuf.readDouble());
    }
    buf.readerIndex(buf.readerIndex() + size * 8);
  }

  public Int2DoubleOpenHashMap getData() {
    return hashMap;
  }

  @Override
  public void serialize(ByteBuf buf) {
    try {
      lock.readLock().lock();
      super.serialize(buf);
      buf.writeInt(hashMap.size());
      for (Int2DoubleMap.Entry entry : hashMap.int2DoubleEntrySet()) {
        buf.writeInt(entry.getIntKey());
        buf.writeDouble(entry.getDoubleValue());
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
      if(hashMap == null){
        hashMap = new Int2DoubleOpenHashMap(elemNum);
      }  
      for (int i = 0; i < elemNum; i++) {
        hashMap.put(buf.readInt(), buf.readDouble());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public int bufferLen() {
    try {
      lock.readLock().lock();
      return super.bufferLen() + 4 + hashMap.size() * 12;
    } finally {
      lock.readLock().unlock();
    }
  }

  public void mergeDoubleDense(int size, ByteBuf buf) {
    try {
      lock.writeLock().lock();
      for (int i = 0; i < size; i++) {
        hashMap.addTo(i, buf.readDouble());
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void mergeDoubleSparse(int size, ByteBuf buf) {
    try {
      lock.writeLock().lock();
      ByteBuf valueBuf = buf.slice(buf.readerIndex() + size * 4, size * 8);
      for (int i = 0; i < size; i++) {
        hashMap.addTo(buf.readInt(), valueBuf.readDouble());
      }
      buf.readerIndex(buf.readerIndex() + size * 8);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void mergeTo(Int2DoubleOpenHashMap indexToValueMap) {
    try {
      lock.readLock().lock();
      indexToValueMap.putAll(hashMap);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void mergeTo(int[] indexes, double[] values, int startPos, int len) {
    try {
      lock.readLock().lock();
      int writeLen = len < hashMap.size() ? len : hashMap.size();
      if (writeLen == 0) {
        return;
      }

      int index = 0;
      for (Int2DoubleMap.Entry entry : hashMap.int2DoubleEntrySet()) {
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
}
