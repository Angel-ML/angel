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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.DoubleBuffer;

/**
 * Default row updater.
 */
public class DefaultRowUpdater extends RowUpdater {
  private final static Log LOG = LogFactory.getLog(DefaultRowUpdater.class);

  @Override
  public void updateIntDenseToIntArbitrary(int size, ByteBuf buf, ServerArbitraryIntRow row) {
    row.updateIntDense(buf, size);
  }

  @Override
  public void updateIntSparseToIntArbitrary(int size, ByteBuf buf, ServerArbitraryIntRow row) {
    row.updateIntSparse(buf, size);
  }

  @Override
  public void updateIntDenseToIntDense(int size, ByteBuf buf, ServerDenseIntRow row) {
    row.update(RowType.T_INT_DENSE, buf, size);
  }

  @Override
  public void updateIntSparseToIntDense(int size, ByteBuf buf, ServerDenseIntRow row) {
    row.update(RowType.T_INT_SPARSE, buf, size);
  }

  @Override
  public void updateIntDenseToIntSparse(int size, ByteBuf buf, ServerSparseIntRow row) {
    row.update(RowType.T_INT_DENSE, buf, size);
  }

  @Override
  public void updateIntSparseToIntSparse(int size, ByteBuf buf, ServerSparseIntRow row) {
    row.update(RowType.T_INT_SPARSE, buf, size);
  }

  @Override
  public void updateDoubleDenseToDoubleDense(int size, ByteBuf buf, ServerDenseDoubleRow row) {
    row.update(RowType.T_DOUBLE_DENSE, buf, size);
  }

  @Override
  public void updateDoubleSparseToDoubleDense(int size, ByteBuf buf, ServerDenseDoubleRow row) {
    row.update(RowType.T_DOUBLE_SPARSE, buf, size);
  }

  @Override
  public void updateDoubleDenseToDoubleSparse(int size, ByteBuf buf, ServerSparseDoubleRow row) {
    row.update(RowType.T_DOUBLE_DENSE, buf, size);
  }

  @Override
  public void updateDoubleSparseToDoubleSparse(int size, ByteBuf buf, ServerSparseDoubleRow row) {
    row.update(RowType.T_DOUBLE_SPARSE, buf, size);
  }

  @Override public void updateDoubleSparseToDoubleSparseLongKey(int size, ByteBuf buf,
    ServerSparseDoubleLongKeyRow row) {
    row.update(RowType.T_DOUBLE_SPARSE_LONGKEY, buf, size);
  }

  @Override
  public void updateFloatDenseToFloatDense(int size, ByteBuf buf, ServerDenseFloatRow row) {
    row.update(RowType.T_FLOAT_DENSE, buf, size);
  }

  @Override
  public void updateFloatSparseToFloatDense(int size, ByteBuf buf, ServerDenseFloatRow row) {
    row.update(RowType.T_FLOAT_SPARSE, buf, size);
  }

  @Override
  public void updateFloatDenseToFloatSparse(int size, ByteBuf buf, ServerSparseFloatRow row) {
    row.update(RowType.T_FLOAT_DENSE, buf, size);
  }

  @Override
  public void updateFloatSparseToFloatSparse(int size, ByteBuf buf, ServerSparseFloatRow row) {
    row.update(RowType.T_FLOAT_SPARSE, buf, size);
  }

  public void updateDoubleDenseToDoubleDense(int size, ByteBuf buf, ServerDenseDoubleRow row,
      int compressRatio) {
    int bitPerItem = 8 * 8 / compressRatio;

    DoubleBuffer data = row.getData();

    LOG.debug("update double to double, size: " + size);

    if (size <= 0)
      return;

    double maxAbs = buf.readDouble();
    int maxPoint = (int) Math.pow(2, bitPerItem - 1) - 1;

    for (int i = 0; i < size - 1; i++) {
      byte[] itemBytes = new byte[bitPerItem / 8];
      buf.readBytes(itemBytes);
      int point = byteArray2int(itemBytes);
      double parsedValue = (double) point / (double) maxPoint * maxAbs;
      data.put(i, data.get(i) + parsedValue);
    }

    LOG.info(String.format("parse compressed %d double data, max abs: %f, max point: %d", size - 1,
        maxAbs, maxPoint));
  }

  private static String byte2hex(byte[] buffer) {
    String h = "";

    for (int i = 0; i < buffer.length; i++) {
      String temp = Integer.toHexString(buffer[i] & 0xFF);
      if (temp.length() == 1) {
        temp = "0" + temp;
      }
      h = h + " " + temp;
    }

    return h.trim();
  }

  private static int byteArray2int(byte[] buffer) {
    int rec = 0;
    boolean isNegative = (buffer[0] & 0x80) == 0x80;
    buffer[0] &= 0x7F; // set the negative flag to 0

    int base = 0;
    for (int i = buffer.length - 1; i >= 0; i--) {
      byte value = buffer[i];
      value <<= base;
      base += 8;
      rec += value;
    }

    if (isNegative) {
      rec = ~rec;
    }

    return rec;
  }
}
