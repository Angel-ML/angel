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


package com.tencent.angel.ps.server.data.response;

import com.tencent.angel.ps.server.data.request.ValueType;
import com.tencent.angel.psagent.matrix.transport.adapter.*;
import io.netty.buffer.ByteBuf;

public class IndexPartGetRowResponse extends Response {
  private volatile IndexPartGetRowResult partResult;

  /**
   * Create a new GetUDFResponse
   *
   * @param responseType response type
   * @param detail       detail failed message if the response is not success
   * @param partResult   result
   */
  public IndexPartGetRowResponse(ResponseType responseType, String detail,
    IndexPartGetRowResult partResult) {
    super(responseType, detail);
    this.partResult = partResult;
  }

  public IndexPartGetRowResponse(ResponseType responseType, String detail) {
    this(responseType, detail, null);
  }

  public IndexPartGetRowResponse(ResponseType responseType) {
    this(responseType, "", null);
  }

  public IndexPartGetRowResponse() {
    this(ResponseType.SUCCESS);
  }

  public IndexPartGetRowResult getPartResult() {
    return partResult;
  }

  public void setPartResult(IndexPartGetRowResult partResult) {
    this.partResult = partResult;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    if (partResult != null) {
      buf.writeInt(partResult.getValueType().getTypeId());
      partResult.serialize(buf);
    }
  }

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    if (buf.isReadable()) {
      ValueType valueType = ValueType.valueOf(buf.readInt());
      switch (valueType) {
        case DOUBLE:
          partResult = new IndexPartGetRowDoubleResult();
          break;
        case FLOAT:
          partResult = new IndexPartGetRowFloatResult();
          break;
        case INT:
          partResult = new IndexPartGetRowIntResult();
          break;
        case LONG:
          partResult = new IndexPartGetRowLongResult();
          break;
        default:
          throw new UnsupportedOperationException("unsupport value type " + valueType);
      }
      partResult.deserialize(buf);
    }
  }

  @Override public int bufferLen() {
    if (partResult == null) {
      return super.bufferLen();
    } else {
      return super.bufferLen() + 4 + partResult.bufferLen();
    }
  }

  /**
   * Clear RPC Get result
   */
  @Override public void clear() {
    partResult = null;
  }
}
