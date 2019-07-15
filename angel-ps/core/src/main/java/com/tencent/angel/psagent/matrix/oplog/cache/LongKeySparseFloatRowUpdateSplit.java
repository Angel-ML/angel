/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.psagent.matrix.oplog.cache;

import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.matrix.RowType;
import io.netty.buffer.ByteBuf;

public class LongKeySparseFloatRowUpdateSplit extends RowUpdateSplit {

  /**
   * indexes
   */
  private final long[] offsets;

  /**
   * values of row
   */
  private final float[] values;

  /**
   * Create a new RowUpdateSplit.
   *
   * @param rowIndex row index
   * @param start split start position
   * @param end split end position
   */
  public LongKeySparseFloatRowUpdateSplit(int rowIndex, int start, int end, long[] offsets,
      float[] values) {
    super(rowIndex, RowType.T_FLOAT_SPARSE_LONGKEY, start, end);
    this.offsets = offsets;
    this.values = values;
  }

  public LongKeySparseFloatRowUpdateSplit() {
    this(-1, -1, -1, null, null);
  }

  /**
   * Get indexes of row values
   *
   * @return int[] indexes of row values
   */
  public long[] getOffsets() {
    return offsets;
  }

  /**
   * Get row values
   *
   * @return double[] row values
   */
  public float[] getValues() {
    return values;
  }

  @Override
  public boolean isUseIntKey() {
    return rowType == RowType.T_FLOAT_SPARSE;
  }

  @Override
  public void setUseIntKey(boolean useIntKey) {
    if (useIntKey) {
      setRowType(RowType.T_FLOAT_SPARSE);
    } else {
      setRowType(RowType.T_FLOAT_SPARSE_LONGKEY);
    }
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    if (isUseIntKey()) {
      long startCol = splitContext.getPartKey().getStartCol();
      if (splitContext.isEnableFilter()) {
        float filterValue = (float) splitContext.getFilterThreshold();
        int position = buf.writerIndex();
        buf.writeInt(0);
        int needUpdateItemNum = 0;
        for (int i = start; i < end; i++) {
          if (Math.abs(values[i]) > filterValue) {
            buf.writeInt((int) (offsets[i] - startCol));
            buf.writeFloat(values[i]);
            needUpdateItemNum++;
          }
        }
        buf.setInt(position, needUpdateItemNum);
      } else {
        buf.writeInt(end - start);
        for (int i = start; i < end; i++) {
          buf.writeInt((int) (offsets[i] - startCol));
          buf.writeFloat(values[i]);
        }
      }
    } else {
      long startCol = splitContext.getPartKey().getStartCol();
      if (splitContext.isEnableFilter()) {
        float filterValue = (float) splitContext.getFilterThreshold();
        int position = buf.writerIndex();
        buf.writeInt(0);
        int needUpdateItemNum = 0;
        for (int i = start; i < end; i++) {
          if (Math.abs(values[i]) > filterValue) {
            buf.writeLong(offsets[i] - startCol);
            buf.writeFloat(values[i]);
            needUpdateItemNum++;
          }
        }
        buf.setInt(position, needUpdateItemNum);
      } else {
        buf.writeInt(end - start);
        for (int i = start; i < end; i++) {
          buf.writeLong(offsets[i] - startCol);
          buf.writeFloat(values[i]);
        }
      }
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int size = buf.readInt();
    if (isUseIntKey()) {
      vector = VFactory.sparseFloatVector(
          (int) (splitContext.getPartKey().getEndCol() - splitContext.getPartKey().getStartCol()),
          size);
      for (int i = 0; i < size; i++) {
        ((IntFloatVector) vector).set(buf.readInt(), buf.readFloat());
      }
    } else {
      vector = VFactory.sparseLongKeyFloatVector(
          splitContext.getPartKey().getEndCol() - splitContext.getPartKey().getStartCol(), size);
      for (int i = 0; i < size; i++) {
        ((LongFloatVector) vector).set(buf.readLong(), buf.readFloat());
      }
    }
  }

  private int getNeedUpdateItemNum() {
    int needUpdateItemNum = 0;
    float filterValue = (float) splitContext.getFilterThreshold();
    for (int i = start; i < end; i++) {
      if (Math.abs(values[i]) > filterValue) {
        needUpdateItemNum++;
      }
    }
    return needUpdateItemNum;
  }

  @Override
  public int bufferLen() {
    if (isUseIntKey()) {
      if (splitContext.isEnableFilter()) {
        return 12 + super.bufferLen() + getNeedUpdateItemNum() * 8;
      } else {
        return 12 + super.bufferLen() + (end - start) * 8;
      }
    } else {
      if (splitContext.isEnableFilter()) {
        return 12 + super.bufferLen() + getNeedUpdateItemNum() * 12;
      } else {
        return 12 + super.bufferLen() + (end - start) * 12;
      }
    }
  }
}
