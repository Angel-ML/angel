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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.util.Arrays;

/**
 * The class represent dense double row on parameter server.
 */
public class ServerDenseDoubleRow extends ServerDoubleRow {
  private final static Log LOG = LogFactory.getLog(ServerDenseDoubleRow.class);

  /** Byte array */
  private byte[] dataBuffer;

  /** The double array view of the byte array */
  private DoubleBuffer data;

  /**
   * Create a new Server dense double row.
   *
   * @param rowId    the row id
   * @param startCol the start col
   * @param endCol   the end col
   */
  public ServerDenseDoubleRow(int rowId, int startCol, int endCol) {
    this(rowId, startCol, endCol, new byte[(endCol - startCol) * 8]);
  }

  /**
   * Create a new Server dense double row.
   *
   * @param rowId    the row id
   * @param startCol the start col
   * @param endCol   the end col
   * @param buffer   the data buffer
   */
  public ServerDenseDoubleRow(int rowId, int startCol, int endCol, byte[] buffer) {
    super(rowId, startCol, endCol);
    int elemNum = endCol - startCol;
    this.dataBuffer = buffer;
    if (dataBuffer != null) {
      this.data = ByteBuffer.wrap(dataBuffer, 0, elemNum * 8).asDoubleBuffer();
    } else {
      dataBuffer = null;
    }
  }

  /**
   * Create a new Server dense double row.
   */
  public ServerDenseDoubleRow() {
    this(0, 0, 0, null);
  }
  
  @Override
  public RowType getRowType() {
    return RowType.T_DOUBLE_DENSE;
  }

  @Override
  public int size() {
    try {
      lock.readLock().lock();
      return dataBuffer.length / 8;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void update(RowType rowType, ByteBuf buf) {
    tryToLockWrite();

    try {
      switch (rowType) {
        case T_DOUBLE_SPARSE:
        case T_DOUBLE_SPARSE_COMPONENT:
          sparseDoubleUpdate(buf);
          break;

        case T_DOUBLE_DENSE:
          denseDoubleUpdate(buf);
          break;

        default:
          throw new UnsupportedOperationException("Unsupport operation: update " + rowType + " to " + this.getClass().getName());
      }

      updateRowVersion();
    } finally {
      unlockWrite();
    }
  }

  private void denseDoubleUpdate(ByteBuf buf) {
    int size = buf.readInt();
    assert size == (endCol - startCol);
    for (int i = 0; i < size; i++) {
      data.put(i, data.get(i) + buf.readDouble());
    }
  }

  private void sparseDoubleUpdate(ByteBuf buf) {
    int columnId;
    double value;
    int startColInt = (int) startCol;
    int size = buf.readInt();
    for (int i = 0; i < size; i++) {
      columnId = buf.readInt() - startColInt;
      value = data.get(columnId) + buf.readDouble();
      data.put(columnId, value);
    }
  }

  /**
   * Get double array view
   * @return double array view
   */
  public DoubleBuffer getData() {
    return data;
  }

  /**
   * Get byte array
   * @return byte array
   */
  public byte[] getDataArray() {
    return dataBuffer;
  }

  @Override
  public void writeTo(DataOutputStream output) throws IOException {
    try {
      //lock.readLock().lock();
      super.writeTo(output);
      output.write(dataBuffer, 0, dataBuffer.length);
    } finally {
      //lock.readLock().unlock();
    }
  }

  @Override
  public void readFrom(DataInputStream input) throws IOException {
    try {
      lock.writeLock().lock();
      super.readFrom(input);
      int totalSize = (int)(endCol - startCol) * 8;
      int size = 0;
      while (size < totalSize) {
        int tempSize = input.read(dataBuffer, size, (totalSize - size));
        size += tempSize;
      }
    } finally {
      lock.writeLock().unlock();
    }
  }


  @Override
  public void serialize(ByteBuf buf) {
    try {
      lock.readLock().lock();
      super.serialize(buf);
      buf.writeInt((int)(endCol - startCol));
      buf.writeBytes(dataBuffer);
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
      if (dataBuffer == null || dataBuffer.length != elemNum * 8) {
        dataBuffer = new byte[elemNum * 8];
      }

      buf.readBytes(dataBuffer);
      this.data = ByteBuffer.wrap(dataBuffer, 0, elemNum * 8).asDoubleBuffer();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public int bufferLen() {
    try {
      lock.readLock().lock();
      return super.bufferLen() + 4 + dataBuffer.length;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override public void reset() {
    try {
      lock.writeLock().lock();
      Arrays.fill(dataBuffer, (byte) 0);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Merge this dense double vector split to a double array
   * @param dataArray  double array for merge
   */
  public void mergeTo(double[] dataArray) {
    try {
      lock.readLock().lock();
      // data.rewind();
      int size = (int) (endCol - startCol);
      int startPos = (int) startCol;
      for (int i = 0; i < size; i++) {
        dataArray[startPos + i] = data.get(i);
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override protected double getValue(int index) {
    return data.get(index - (int) startCol);
  }
}
