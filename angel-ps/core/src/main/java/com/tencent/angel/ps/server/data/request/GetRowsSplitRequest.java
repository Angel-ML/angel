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


package com.tencent.angel.ps.server.data.request;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.TransportMethod;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Get a batch row splits rpc request.
 */
public class GetRowsSplitRequest extends PartitionRequest {
  /**
   * row indexes
   */
  private List<Integer> rowIndexes;

  /**
   * Create a new GetRowsSplitRequest.
   *
   * @param userRequestId user request id
   * @param clock         clock value
   * @param partKey       matrix partition key
   * @param rowIndexes    row indexes
   */
  public GetRowsSplitRequest(int userRequestId, int clock, PartitionKey partKey,
    List<Integer> rowIndexes) {
    super(userRequestId, clock, partKey);
    this.rowIndexes = rowIndexes;
  }

  /**
   * Create a new GetRowsSplitRequest.
   */
  public GetRowsSplitRequest() {
    this(-1, 0, null, null);
  }

  @Override public int getEstimizeDataSize() {
    MatrixMeta meta =
      PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(partKey.getMatrixId());
    if (meta == null || rowIndexes == null) {
      return 0;
    } else {
      RowType rowType = meta.getRowType();
      switch (rowType) {
        case T_DOUBLE_DENSE:
          return 8 * ((int) partKey.getEndCol() - (int) partKey.getStartCol()) * rowIndexes.size();

        case T_FLOAT_DENSE:
          return 4 * ((int) partKey.getEndCol() - (int) partKey.getStartCol()) * rowIndexes.size();

        case T_INT_DENSE:
          return 4 * ((int) partKey.getEndCol() - (int) partKey.getStartCol()) * rowIndexes.size();

        default: {
          return 0;
        }
      }
    }
  }

  @Override public TransportMethod getType() {
    return TransportMethod.GET_ROWSSPLIT;
  }

  /**
   * Get row indexes.
   *
   * @return List<Integer> row indexes
   */
  public List<Integer> getRowIndexes() {
    return rowIndexes;
  }

  /**
   * Set row indexes.
   *
   * @param rowIndexes row indexes
   */
  public void setRowIndexes(List<Integer> rowIndexes) {
    this.rowIndexes = rowIndexes;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    if (rowIndexes != null) {
      int size = rowIndexes.size();
      buf.writeInt(size);
      for (int i = 0; i < size; i++) {
        buf.writeInt(rowIndexes.get(i));
      }
    }
  }

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    if (buf.readableBytes() != 0) {
      int size = buf.readInt();
      rowIndexes = new ArrayList<Integer>(size);
      for (int i = 0; i < size; i++) {
        rowIndexes.add(buf.readInt());
      }
    }
  }

  @Override public int bufferLen() {
    return super.bufferLen() + (rowIndexes != null ? (4 + rowIndexes.size() * 4) : 0);
  }

  @Override public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((rowIndexes == null) ? 0 : rowIndexes.size());
    return result;
  }

  @Override public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    GetRowsSplitRequest other = (GetRowsSplitRequest) obj;
    if (rowIndexes == null) {
      if (other.rowIndexes != null)
        return false;
    } else if (!equals(rowIndexes, other.rowIndexes))
      return false;
    return true;
  }

  private boolean equals(List<Integer> list1, List<Integer> list2) {
    if (list1 == null && list2 == null) {
      return true;
    } else if (list1 != null && list2 != null) {
      if (list1.size() != list2.size()) {
        return false;
      }

      int size = list1.size();
      ArrayList<Integer> sortedList1 = new ArrayList<Integer>(size);
      ArrayList<Integer> sortedList2 = new ArrayList<Integer>(size);
      sortedList1.addAll(list1);
      sortedList2.addAll(list2);
      Collections.sort(sortedList1);
      Collections.sort(sortedList2);

      for (int i = 0; i < size; i++) {
        if (!Objects.equals(sortedList1.get(i), sortedList2.get(i))) {
          return false;
        }
      }

      return true;
    } else {
      return false;
    }
  }
}