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

import com.tencent.angel.ml.matrix.RowType;
import io.netty.buffer.ByteBuf;

public class LongKeySparseDoubleRowUpdateSplit extends RowUpdateSplit {
  /**
   * indexes
   */
  private final long[] offsets;

  /**
   * values of row
   */
  private final double[] values;

  /**
   * Create a new RowUpdateSplit.
   *
   * @param rowIndex row index
   * @param start    split start position
   * @param end      split end position
   */
  public LongKeySparseDoubleRowUpdateSplit(int rowIndex, int start, int end, long[] offsets,
    double[] values) {
    super(rowIndex, RowType.T_DOUBLE_SPARSE_LONGKEY, start, end);
    this.offsets = offsets;
    this.values = values;
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
  public double[] getValues() {
    return values;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    long startCol = splitContext.getPartKey().getStartCol();
    //buf.writeDouble(0.0);
    if (splitContext.isEnableFilter()) {
      double filterValue = splitContext.getFilterThreshold();
      int position = buf.writerIndex();
      buf.writeInt(0);
      int needUpdateItemNum = 0;
      for (int i = start; i < end; i++) {
        if (Math.abs(values[i]) > filterValue) {
          buf.writeLong(offsets[i] - startCol);
          buf.writeDouble(values[i]);
          needUpdateItemNum++;
        }
      }
      buf.setInt(position, needUpdateItemNum);
    } else {
      buf.writeInt(end - start);
      for (int i = start; i < end; i++) {
        buf.writeLong(offsets[i] - startCol);
        buf.writeDouble(values[i]);
      }
    }
  }

  private int getNeedUpdateItemNum() {
    int needUpdateItemNum = 0;
    double filterValue = splitContext.getFilterThreshold();
    for (int i = start; i < end; i++) {
      if (Math.abs(values[i]) > filterValue) {
        needUpdateItemNum++;
      }
    }
    return needUpdateItemNum;
  }

  @Override public int bufferLen() {
    if (splitContext.isEnableFilter()) {
      return 12 + super.bufferLen() + getNeedUpdateItemNum() * 16;
    } else {
      return 12 + super.bufferLen() + (end - start) * 16;
    }
  }
}
