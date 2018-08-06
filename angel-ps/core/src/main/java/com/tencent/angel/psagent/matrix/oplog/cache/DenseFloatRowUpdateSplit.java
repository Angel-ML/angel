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

/**
 * Row split of dense double float update.
 */
public class DenseFloatRowUpdateSplit extends RowUpdateSplit {

  /** values of row */
  private final float [] values;

  /**
   * Create a new dense float row split update
   *
   * @param start start position
   * @param end end position
   * @param values values of row update
   */
  public DenseFloatRowUpdateSplit(int rowIndex, int start, int end, float [] values) {
    super(rowIndex, RowType.T_FLOAT_DENSE, start, end);
    this.values = values;
  }

  /**
   * Get values of row update
   * 
   * @return float[] values of row update
   */
  public float[] getValues() {
    return values;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt((int)(end - start));
    for (int i = (int)start; i < end; i++) {
      buf.writeFloat(values[i]);
    }
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + (int)size() * 4;
  }
}
