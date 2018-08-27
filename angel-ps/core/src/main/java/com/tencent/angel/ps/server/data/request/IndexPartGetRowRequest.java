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
import com.tencent.angel.ps.server.data.TransportMethod;
import com.tencent.angel.psagent.matrix.transport.adapter.IndicesView;
import com.tencent.angel.psagent.matrix.transport.adapter.IntIndicesView;
import io.netty.buffer.ByteBuf;

public class IndexPartGetRowRequest extends PartitionRequest {
  private int matrixId;
  private int rowId;
  private final IndicesView colIds;
  private final ValueType valueType;

  public IndexPartGetRowRequest(int userRequestId, int matrixId, int rowId, PartitionKey partKey,
    IndicesView colIds, ValueType valueType) {
    super(userRequestId, -1, partKey);
    this.matrixId = matrixId;
    this.rowId = rowId;
    this.colIds = colIds;
    this.valueType = valueType;
  }

  public IndexPartGetRowRequest() {
    this(-1, -1, -1, null, null, ValueType.DOUBLE);
  }

  public int getMatrixId() {
    return matrixId;
  }

  public int getRowId() {
    return rowId;
  }

  @Override public int getEstimizeDataSize() {
    if (valueType == ValueType.INT || valueType == ValueType.FLOAT) {
      return 4 * (colIds.endPos - colIds.startPos);
    } else {
      return 8 * (colIds.endPos - colIds.startPos);
    }
  }

  @Override public TransportMethod getType() {
    return TransportMethod.INDEX_GET_ROW;
  }

  public IndicesView getColIds() {
    return colIds;
  }

  public ValueType getValueType() {
    return valueType;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(matrixId);
    buf.writeInt(rowId);
    if (colIds instanceof IntIndicesView) {
      buf.writeInt(IndexType.INT.getTypeId());
    } else {
      buf.writeInt(IndexType.LONG.getTypeId());
    }
    colIds.serialize(buf);
  }

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    matrixId = buf.readInt();
    rowId = buf.readInt();
  }

  @Override public int bufferLen() {
    if (colIds != null) {
      return super.bufferLen() + 12 + colIds.bufferLen();
    } else {
      return super.bufferLen() + 8;
    }
  }

  @Override public int getHandleElemNum() {
    if (colIds != null) {
      handleElemSize = colIds.endPos - colIds.startPos;
    }
    return handleElemSize;
  }
}
