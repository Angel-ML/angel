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
import com.tencent.angel.ml.math2.utils.RowType;
import io.netty.buffer.ByteBuf;

/**
 * Row split of dense int row update.
 */
public class DenseIntRowUpdateSplit extends RowUpdateSplit {

  /**
   * values of row
   */
  private final int[] values;

  /**
   * Create a new dense int row split update
   *
   * @param start start position
   * @param end end position
   * @param values values of row update
   */
  public DenseIntRowUpdateSplit(int rowIndex, int start, int end, int[] values) {
    super(rowIndex, RowType.T_INT_DENSE, start, end);
    this.values = values;
  }

  public DenseIntRowUpdateSplit() {
    this(-1, -1, -1, null);
  }

  /**
   * Get values of row update
   *
   * @return int[] values of row update
   */
  public int[] getValues() {
    return values;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(end - start);
    for (int i = start; i < end; i++) {
      buf.writeInt(values[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int[] data = new int[buf.readInt()];
    for (int i = 0; i < data.length; i++) {
      data[i] = buf.readInt();
    }
    vector = VFactory.denseIntVector(data);
  }


  @Override
  public int bufferLen() {
    return 4 + super.bufferLen() + (end - start) * 4;
  }
}
