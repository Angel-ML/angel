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

package com.tencent.angel.psagent.matrix.oplog.cache;

import com.tencent.angel.protobuf.generated.MLProtos.RowType;
import io.netty.buffer.ByteBuf;

/**
 * Row split of dense double row update.
 */
public class DenseDoubleRowUpdateSplit extends RowUpdateSplit {

  /** values of row */
  private final double[] values;

  /**
   * Create a new dense double row split update
   *
   * @param start start position
   * @param end end position
   * @param values values of row update
   */
  public DenseDoubleRowUpdateSplit(int rowIndex, int start, int end, double[] values) {
    super(rowIndex, RowType.T_DOUBLE_DENSE, start, end);
    this.values = values;
  }

  /**
   * Get values of row update
   * 
   * @return double[] values of row update
   */
  public double[] getValues() {
    return values;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(end - start);
    LOG.debug("double size = " + (end - start));
    for (int i = start; i < end; i++) {
      buf.writeDouble(values[i]);
    }
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + size() * 8;
  }
}
