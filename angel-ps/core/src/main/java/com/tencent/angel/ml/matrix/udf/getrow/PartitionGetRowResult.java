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

package com.tencent.angel.ml.matrix.udf.getrow;

import com.tencent.angel.common.Serialize;
import com.tencent.angel.ps.impl.matrix.*;
import com.tencent.angel.protobuf.generated.MLProtos.RowType;
import io.netty.buffer.ByteBuf;

/**
 * The result of partition get row.
 */
public class PartitionGetRowResult implements Serialize{
  private ServerRow rowSplit;

  /**
   * Creates a new partition result.
   *
   * @param rowSplit the row split
   */
  public PartitionGetRowResult(ServerRow rowSplit) {
    this.rowSplit = rowSplit;
  }

  /**
   * Creates a new partition result by default.
   */
  public PartitionGetRowResult() {
    this(null);
  }

  @Override
  public void serialize(ByteBuf buf) {
    if (rowSplit != null) {
      buf.writeInt(rowSplit.getRowType().getNumber());
      rowSplit.serialize(buf);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    if (buf.readableBytes() == 0) {
      rowSplit = null;
      return;
    }

    RowType type = RowType.valueOf(buf.readInt());
    if (rowSplit == null) {
      switch (type) {
        case T_DOUBLE_DENSE: {
          rowSplit = new ServerDenseDoubleRow();
          break;
        }
        case T_DOUBLE_SPARSE: {
          rowSplit = new ServerSparseDoubleRow();
          break;
        }

        case T_INT_DENSE: {
          rowSplit = new ServerDenseIntRow();
          break;
        }

        case T_INT_SPARSE: {
          rowSplit = new ServerSparseIntRow();
          break;
        }

        case T_FLOAT_DENSE: {
          rowSplit = new ServerDenseFloatRow();
          break;
        }
        default:
          break;
      }
    }

    rowSplit.deserialize(buf);
  }

  @Override
  public int bufferLen() {
    return rowSplit.bufferLen();
  }

  /**
   * Gets row split.
   *
   * @return the row split
   */
  public ServerRow getRowSplit() {
    return rowSplit;
  }

  /**
   * Sets row split.
   *
   * @param rowSplit the row split
   */
  public void setRowSplit(ServerRow rowSplit) {
    this.rowSplit = rowSplit;
  }
}
