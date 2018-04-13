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
import java.nio.IntBuffer;
import java.util.Arrays;

/**
 * The class represent dense int row on parameter server.
 */
public class ServerDenseIntRow extends ServerIntRow {
  private final static Log LOG = LogFactory.getLog(ServerDenseIntRow.class);

  /** Byte array */
  private byte[] dataBuffer;

  /** The double array view of the byte array */
  private IntBuffer data;

  /**
   * Create a ServerDenseIntRow
   * @param rowId row index
   * @param startCol partition start column index
   * @param endCol partition end column index
   * @param buffer byte buffer
   */
  public ServerDenseIntRow(int rowId, int startCol, int endCol, byte[] buffer) {
    super(rowId, startCol, endCol);
    int elemNum = endCol - startCol;
    this.dataBuffer = buffer;
    if (dataBuffer != null) {
      this.data = ByteBuffer.wrap(this.dataBuffer, 0, elemNum * 4).asIntBuffer();
    } else {
      this.data = null;
    }
  }

  /**
   * Create a ServerDenseIntRow
   * @param rowId row index
   * @param startCol partition start column index
   * @param endCol partition end column index
   */
  public ServerDenseIntRow(int rowId, int startCol, int endCol) {
    this(rowId, startCol, endCol, new byte[(endCol - startCol) * 4]);
  }

  /**
   * Create a ServerDenseFloatRow
   */
  public ServerDenseIntRow() {
    this(0, 0, 0, null);
  }

  @Override protected int getValue(int index) {
    return data.get(index - (int) startCol);
  }

  @Override
  public RowType getRowType() {
    return RowType.T_INT_DENSE;
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
      int totalSize = (int)(endCol - startCol) * 4;
      int readLen = 0;
      int size = 0;
      while (size < totalSize) {
        readLen = input.read(dataBuffer, size, (totalSize - size));
        size += readLen;
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public int size() {
    return dataBuffer.length / 4;
  }

  @Override
  public void update(RowType rowType, ByteBuf buf) {
    tryToLockWrite();

    try {
      switch (rowType) {
        case T_INT_DENSE:
          updateIntDense(buf);
          break;

        case T_INT_SPARSE:
        case T_INT_SPARSE_COMPONENT:
          updateIntSparse(buf);
          break;

        default:
          throw new UnsupportedOperationException("Unsupport operation: update " + rowType + " to " + this.getClass().getName());
      }
      updateRowVersion();
    } finally {
      unlockWrite();
    }
  }

  private void updateIntDense(ByteBuf buf) {
    int size = buf.readInt();
    for (int colId = 0; colId < size; colId++) {
      data.put(colId, data.get(colId) + buf.readInt());
    }
  }

  private void updateIntSparse(ByteBuf buf) {
    int columnId;
    int value;
    int startColInt = (int) startCol;
    int size = buf.readInt();
    for (int i = 0; i < size; i++) {
      columnId = buf.readInt()- startColInt;
      value = data.get(columnId) + buf.readInt();
      data.put(columnId, value);
    }
  }

  public IntBuffer getData() {
    return data;
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
      if(dataBuffer == null || dataBuffer.length != elemNum * 4){
        dataBuffer = new byte[elemNum * 4];
      }
      
      buf.readBytes(dataBuffer);
      this.data = ByteBuffer.wrap(dataBuffer, 0, elemNum * 4).asIntBuffer();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + 4 + dataBuffer.length;
  }

  @Override public void reset() {
    try {
      lock.writeLock().lock();
      Arrays.fill(dataBuffer, (byte)0);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Merge this dense int vector to a float array
   * @param dataArray float array
   */
  public void mergeTo(int[] dataArray) {
    try {
      lock.readLock().lock();
      //data.rewind();
      int size = (int)(endCol - startCol);
      int startPos = (int) startCol;
      for (int i = 0; i < size; i++) {
        dataArray[startPos + i] = data.get(i);
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  public byte[] getDataArray() {
    return dataBuffer;
  }
}
