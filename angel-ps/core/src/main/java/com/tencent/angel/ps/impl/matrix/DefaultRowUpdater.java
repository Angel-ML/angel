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

/**
 * Default row updater.
 */
public class DefaultRowUpdater extends RowUpdater {
  private final static Log LOG = LogFactory.getLog(DefaultRowUpdater.class);

  @Override
  public void updateIntDenseToIntArbitrary(ByteBuf buf, ServerArbitraryIntRow row) {
    row.updateIntDense(buf);
  }

  @Override
  public void updateIntSparseToIntArbitrary(ByteBuf buf, ServerArbitraryIntRow row) {
    row.updateIntSparse(buf);
  }

  @Override
  public void updateIntDenseToIntDense(ByteBuf buf, ServerDenseIntRow row) {
    row.update(RowType.T_INT_DENSE, buf);
  }

  @Override
  public void updateIntSparseToIntDense(ByteBuf buf, ServerDenseIntRow row) {
    row.update(RowType.T_INT_SPARSE, buf);
  }

  @Override
  public void updateIntDenseToIntSparse(ByteBuf buf, ServerSparseIntRow row) {
    row.update(RowType.T_INT_DENSE, buf);
  }

  @Override
  public void updateIntSparseToIntSparse(ByteBuf buf, ServerSparseIntRow row) {
    row.update(RowType.T_INT_SPARSE, buf);
  }

  @Override
  public void updateDoubleDenseToDoubleDense(ByteBuf buf, ServerDenseDoubleRow row) {
    row.update(RowType.T_DOUBLE_DENSE, buf);
  }

  @Override
  public void updateDoubleSparseToDoubleDense(ByteBuf buf, ServerDenseDoubleRow row) {
    row.update(RowType.T_DOUBLE_SPARSE, buf);
  }

  @Override
  public void updateDoubleDenseToDoubleSparse(ByteBuf buf, ServerSparseDoubleRow row) {
    row.update(RowType.T_DOUBLE_DENSE, buf);
  }

  @Override
  public void updateDoubleSparseToDoubleSparse(ByteBuf buf, ServerSparseDoubleRow row) {
    row.update(RowType.T_DOUBLE_SPARSE, buf);
  }

  @Override public void updateDoubleSparseToDoubleSparseLongKey(ByteBuf buf,
    ServerSparseDoubleLongKeyRow row) {
    row.update(RowType.T_DOUBLE_SPARSE_LONGKEY, buf);
  }

  @Override
  public void updateFloatDenseToFloatDense(ByteBuf buf, ServerDenseFloatRow row) {
    row.update(RowType.T_FLOAT_DENSE, buf);
  }

  @Override
  public void updateFloatSparseToFloatDense(ByteBuf buf, ServerDenseFloatRow row) {
    row.update(RowType.T_FLOAT_SPARSE, buf);
  }

  @Override
  public void updateFloatDenseToFloatSparse(ByteBuf buf, ServerSparseFloatRow row) {
    row.update(RowType.T_FLOAT_DENSE, buf);
  }

  @Override
  public void updateFloatSparseToFloatSparse(ByteBuf buf, ServerSparseFloatRow row) {
    row.update(RowType.T_FLOAT_SPARSE, buf);
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
