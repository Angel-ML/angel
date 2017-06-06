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
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;

/**
 * The class represent dense double row on parameter server.
 */
public class ServerDenseDoubleRow extends ServerRow {

  private final static Log LOG = LogFactory.getLog(ServerDenseDoubleRow.class);

  private byte[] dataBuffer;
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
  public MLProtos.RowType getRowType() {
    return MLProtos.RowType.T_DOUBLE_DENSE;
  }

  @Override
  public int size() {
    return dataBuffer.length / 8;
  }

  @Override
  public void update(MLProtos.RowType rowType, ByteBuf buf, int size) {
    try {
      lock.writeLock().lock();
      switch (rowType) {
        case T_DOUBLE_SPARSE:
          sparseDoubleUpdate(buf, size);
          break;

        case T_DOUBLE_DENSE:
          denseDoubleUpdate(buf, size);
          break;

        default:
          break;
      }

      updateRowVersion();
    } finally {
      lock.writeLock().unlock();
    }
  }
  
  public void set(int index, double value) {
    data.put(index, value);
  }

  private void denseDoubleUpdate(ByteBuf buf, int size) {
    assert size == (endCol - startCol);
    for (int i = 0; i < size; i++) {
      data.put(i, data.get(i) + buf.readDouble());
    }
  }

  private void sparseDoubleUpdate(ByteBuf buf, int size) {
    ByteBuf valueBuf = buf.slice(buf.readerIndex() + size * 4, size * 8);
    int columnId = 0;
    double value = 0;
    for (int i = 0; i < size; i++) {
      columnId = buf.readInt();
      value = data.get(columnId) + valueBuf.readDouble();
      data.put(columnId, value);
    }
    buf.readerIndex(buf.readerIndex() + size * 8);
  }

  public DoubleBuffer getData() {
    return data;
  }

  public byte[] getDataArray() {
    return dataBuffer;
  }

  @Override
  public void writeTo(DataOutputStream output) throws IOException {
    try {
      lock.readLock().lock();
      if (LOG.isDebugEnabled()) {
        int size = data.capacity();
        if (size > 0) {
          LOG.debug("server row " + this.toString() + " first element is " + data.get(0));
        } else {
          LOG.debug("server row " + this.toString() + " is empty");
        }
      }
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
      int totalSize = (endCol - startCol) * 8;
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
      buf.writeInt(endCol - startCol);
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
    return super.bufferLen() + 4 + dataBuffer.length;
  }

  public void mergeTo(double[] dataArray) {
    try {
      lock.readLock().lock();
      // data.rewind();
      int size = endCol - startCol;
      for (int i = 0; i < size; i++) {
        dataArray[startCol + i] = data.get(i);
      }
    } finally {
      lock.readLock().unlock();
    }
  }
}
