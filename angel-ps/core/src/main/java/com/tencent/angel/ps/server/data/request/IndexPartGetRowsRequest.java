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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class IndexPartGetRowsRequest extends PartitionRequest {
  private int matrixId;
  private List<Integer> rowIds;
  private final IndicesView colIds;
  private final ValueType valueType;
  private InitFunc func;

  public IndexPartGetRowsRequest(int userRequestId, int matrixId, List<Integer> rowIds,
    PartitionKey partKey, IndicesView colIds, ValueType valueType, InitFunc func) {
    super(userRequestId, -1, partKey);
    this.matrixId = matrixId;
    this.rowIds = rowIds;
    this.colIds = colIds;
    this.valueType = valueType;
    this.func = func;
  }

  public IndexPartGetRowsRequest() {
    this(-1, -1, null, null, null, ValueType.DOUBLE, null);
  }

  public int getMatrixId() {
    return matrixId;
  }

  @Override public int getEstimizeDataSize() {
    if (valueType == ValueType.INT || valueType == ValueType.FLOAT) {
      return 4 * (colIds.endPos - colIds.startPos) * rowIds.size();
    } else {
      return 8 * (colIds.endPos - colIds.startPos) * rowIds.size();
    }
  }

  @Override public TransportMethod getType() {
    return TransportMethod.INDEX_GET_ROWS;
  }

  public ValueType getValueType() {
    return valueType;
  }

  public List<Integer> getRowIds() {
    return rowIds;
  }

  public IndicesView getColIds() {
    return colIds;
  }

  public InitFunc getFunc() {
    return func;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(matrixId);
    buf.writeBoolean(func != null);
    if(func != null) {
      byte[] data = func.getClass().getName().getBytes();
      buf.writeInt(data.length);
      buf.writeBytes(data);
      func.serialize(buf);
    }

    int rowNum = rowIds.size();
    buf.writeInt(rowNum);
    for (int i = 0; i < rowNum; i++) {
      buf.writeInt(rowIds.get(i));
    }

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

    boolean useInitFunc = buf.readBoolean();
    if(useInitFunc) {
      int size = buf.readInt();
      byte[] data = new byte[size];
      buf.readBytes(data);
      String initFuncClass = new String(data);
      try {
        func = (InitFunc) Class.forName(initFuncClass).newInstance();
      } catch (Throwable e) {
        throw new UnsupportedOperationException(e);
      }
      func.deserialize(buf);
    }

    int rowNum = buf.readInt();
    rowIds = new ArrayList<>(rowNum);
    for (int i = 0; i < rowNum; i++) {
      rowIds.add(buf.readInt());
    }
  }

  @Override public int bufferLen() {
    int len = super.bufferLen() + 12 + rowIds.size() * 4 + colIds.bufferLen();
    if(func != null) {
      len += (func.bufferLen() + 8 + func.getClass().getName().getBytes().length);
    }
    return len;
  }

  @Override public int getHandleElemNum() {
    if (rowIds != null && colIds != null) {
      handleElemSize = rowIds.size() * (colIds.endPos - colIds.startPos);
    }
    return handleElemSize;
  }
}
