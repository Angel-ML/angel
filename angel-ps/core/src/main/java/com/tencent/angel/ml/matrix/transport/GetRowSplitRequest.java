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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.ml.matrix.transport;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.protobuf.generated.MLProtos.RowType;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;

/**
 * Get a row split rpc request.
 */
public class GetRowSplitRequest extends PartitionRequest {

  /**
   * row index
   */
  private int rowIndex;

  /**
   * Create a new GetRowSplitRequest.
   *
   * @param serverId parameter server id
   * @param clock    clock value
   * @param partKey  matrix partition key
   * @param rowIndex row index
   */
  public GetRowSplitRequest(ParameterServerId serverId, int clock, PartitionKey partKey,
    int rowIndex) {
    super(serverId, clock, partKey);
    this.rowIndex = rowIndex;
  }

  /**
   * Create a new GetRowSplitRequest.
   */
  public GetRowSplitRequest() {
    this(null, 0, null, 0);
  }

  /**
   * Get row index.
   *
   * @return int row index
   */
  public int getRowIndex() {
    return rowIndex;
  }

  /**
   * Set row index.
   *
   * @param rowIndex row index
   */
  public void setRowIndex(int rowIndex) {
    this.rowIndex = rowIndex;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(rowIndex);
  }

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    rowIndex = buf.readInt();
  }

  @Override public int bufferLen() {
    return super.bufferLen() + 4;
  }

  @Override public TransportMethod getType() {
    return TransportMethod.GET_ROWSPLIT;
  }

  @Override public int getEstimizeDataSize() {
    MatrixMeta meta =
      PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(partKey.getMatrixId());
    if (meta == null) {
      return 0;
    } else {
      RowType rowType = meta.getRowType();
      switch (rowType) {
        case T_DOUBLE_DENSE:
          return 8 * ((int) partKey.getEndCol() - (int) partKey.getStartCol());

        case T_INT_DENSE:
          return 4 * ((int) partKey.getEndCol() - (int) partKey.getStartCol());

        case T_FLOAT_DENSE:
          return 4 * ((int) partKey.getEndCol() - (int) partKey.getStartCol());

        default: {
          ServerRow row = PSAgentContext.get().getMatricesCache()
            .getRowSplit(partKey.getMatrixId(), partKey, rowIndex);
          if (row != null) {
            return row.bufferLen();
          } else {
            return 0;
          }
        }
      }
    }
  }

  @Override public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + rowIndex;
    return result;
  }

  @Override public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    GetRowSplitRequest other = (GetRowSplitRequest) obj;
    return rowIndex == other.rowIndex;
  }

  @Override public String toString() {
    return "GetRowSplitRequest [rowIndex=" + rowIndex + ", toString()=" + super.toString() + "]";
  }
}
