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

import java.util.Random;

public class IndexPartGetRowRequest extends PartitionRequest {
  private int matrixId;
  private int rowId;
  private final IndicesView colIds;
  private final ValueType valueType;
  private InitFunc func;
  private final int hashCode;
  private static final Random r = new Random();

  public IndexPartGetRowRequest(int userRequestId, int matrixId, int rowId, PartitionKey partKey,
    IndicesView colIds, ValueType valueType, InitFunc func) {
    super(userRequestId, -1, partKey);
    this.matrixId = matrixId;
    this.rowId = rowId;
    this.colIds = colIds;
    this.valueType = valueType;
    this.func = func;
    hashCode = r.nextInt();
  }

  public IndexPartGetRowRequest() {
    this(-1, -1, -1, null, null, ValueType.DOUBLE, null);
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

  public InitFunc getFunc() {
    return func;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(matrixId);
    buf.writeInt(rowId);

    buf.writeBoolean(func != null);
    if(func != null) {
      byte[] data = func.getClass().getName().getBytes();
      buf.writeInt(data.length);
      buf.writeBytes(data);
      func.serialize(buf);
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
    rowId = buf.readInt();
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
  }

  @Override public int bufferLen() {
    int len = 0;
    if (colIds != null) {
      len += (super.bufferLen() + 12 + colIds.bufferLen());
    } else {
      len += (super.bufferLen() + 8);
    }

    if(func != null) {
      len += (func.bufferLen() + 8 + func.getClass().getName().getBytes().length);
    }
    return len;
  }

  @Override public int getHandleElemNum() {
    if (colIds != null) {
      handleElemSize = colIds.endPos - colIds.startPos;
    }
    return handleElemSize;
  }

  @Override public boolean equals(Object o) {
    return false;
  }

  @Override public int hashCode() {
    return hashCode;
  }
}
