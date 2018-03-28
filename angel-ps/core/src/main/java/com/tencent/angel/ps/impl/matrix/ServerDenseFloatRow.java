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
import java.nio.FloatBuffer;
import java.util.Arrays;

/**
 * The class represent dense float row on parameter server.
 */
public class ServerDenseFloatRow extends ServerFloatRow {

  private final static Log LOG = LogFactory.getLog(ServerDenseFloatRow.class);
  /** Byte array */
  private byte[] dataBuffer;

  /** The double array view of the byte array */
  private FloatBuffer data;

  /**
   * Create a ServerDenseFloatRow
   * @param rowId row index
   * @param startCol partition start column index
   * @param endCol partition end column index
   */
  public ServerDenseFloatRow(int rowId, int startCol, int endCol) {
    this(rowId, startCol, endCol, new byte[(endCol - startCol) * 4]);
  }

  /**
   *
   * Create a ServerDenseFloatRow
   * @param rowId row index
   * @param startCol partition start column index
   * @param endCol partition end column index
   * @param buffer byte buffer
   */
  public ServerDenseFloatRow(int rowId, int startCol, int endCol, byte[] buffer) {
    super(rowId, startCol, endCol);
    int elemNum = endCol - startCol;
    this.dataBuffer = buffer;
    if (dataBuffer != null) {
      this.data = ByteBuffer.wrap(dataBuffer, 0, elemNum * 4).asFloatBuffer();
    } else {
      dataBuffer = null;
    }
  }

  /**
   * Create a ServerDenseFloatRow
   */
  public ServerDenseFloatRow() {
    this(0, 0, 0, null);
  }

  @Override protected float getValue(int index) {
    return data.get(index - (int) startCol);
  }

  @Override
  public RowType getRowType() {
    return RowType.T_FLOAT_DENSE;
  }

  @Override
  public int size() {
    try {
      lock.readLock().lock();
      return dataBuffer.length / 4;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void update(RowType rowType, ByteBuf buf) {
    tryToLockWrite();

    try {
      switch (rowType) {
        case T_FLOAT_DENSE:
          denseFloatUpdate(buf);
          break;

        case T_FLOAT_SPARSE:
        case T_FLOAT_SPARSE_COMPONENT:
          sparseFloatUpdate(buf);
          break;

        default:
          throw new UnsupportedOperationException("Unsupport operation: update " + rowType + " to " + this.getClass().getName());
      }

      updateRowVersion();
    } finally {
      unlockWrite();
    }
  }

  private void denseFloatUpdate(ByteBuf buf) {
    int size = buf.readInt();
    assert size == (endCol - startCol);
    for (int i = 0; i < size; i++) {
      data.put(i, data.get(i) + buf.readFloat());
    }
  }

  private void sparseFloatUpdate(ByteBuf buf) {
    int columnId;
    float value;
    int startColInt = (int) startCol;
    int size = buf.readInt();
    for (int i = 0; i < size; i++) {
      columnId = buf.readInt() - startColInt;
      value = data.get(columnId) + buf.readFloat();
      data.put(columnId, value);
    }
  }

  /**
   * Get float array view
   * @return float array view
   */
  public FloatBuffer getData() {
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
      lock.readLock().lock();
      super.writeTo(output);
      output.write(dataBuffer, 0, dataBuffer.length);
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void readFrom(DataInputStream input) throws IOException {
    try {
      lock.writeLock().lock();
      super.readFrom(input);
      int totalSize = (int)(endCol - startCol) * 4;
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
      if (dataBuffer == null || dataBuffer.length != elemNum * 4) {
        dataBuffer = new byte[elemNum * 4];
      }

      buf.readBytes(dataBuffer);
      this.data = ByteBuffer.wrap(dataBuffer, 0, elemNum * 4).asFloatBuffer();
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
      Arrays.fill(dataBuffer, (byte) 0);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Merge this dense float vector to a float array
   * @param dataArray float array
   */
  public void mergeTo(float[] dataArray) {
    try {
      lock.readLock().lock();
      // data.rewind();
      int size = (int)(endCol - startCol);
      int startPos = (int) startCol;
      for (int i = 0; i < size; i++) {
        dataArray[startPos + i] = data.get(i);
      }
    } finally {
      lock.readLock().unlock();
    }
  }
}
