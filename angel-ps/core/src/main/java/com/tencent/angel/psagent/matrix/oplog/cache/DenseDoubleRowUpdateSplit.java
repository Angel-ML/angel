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
import com.tencent.angel.ml.matrix.RowType;
import io.netty.buffer.ByteBuf;

/**
 * Row split of dense double row update.
 */
public class DenseDoubleRowUpdateSplit extends RowUpdateSplit {
  /**
   * values of row
   */
  private final double[] values;

  /**
   * Create a new dense double row split update
   *
   * @param start  start position
   * @param end    end position
   * @param values values of row update
   */
  public DenseDoubleRowUpdateSplit(int rowIndex, int start, int end, double[] values) {
    super(rowIndex, RowType.T_DOUBLE_DENSE, start, end);
    this.values = values;
  }

  public DenseDoubleRowUpdateSplit() {
    this(-1, -1, -1, null);
  }

  /**
   * Get values of row update
   *
   * @return double[] values of row update
   */
  public double[] getValues() {
    return values;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(end - start);
    for (int i = start; i < end; i++) {
      buf.writeDouble(values[i]);
    }
  }

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    double [] data = new double[buf.readInt()];
    for(int i = 0; i < data.length; i++) {
      data[i] = buf.readDouble();
    }
    vector = VFactory.denseDoubleVector(data);
  }

  @Override public int bufferLen() {
    return 4 + super.bufferLen() + (end - start) * 8;
  }
}
