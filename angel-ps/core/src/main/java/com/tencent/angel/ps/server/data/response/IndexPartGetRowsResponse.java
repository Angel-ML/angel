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

public class IndexPartGetRowsResponse extends Response {
  private volatile IndexPartGetRowsResult partResult;

  /**
   * Create a new GetUDFResponse
   *
   * @param responseType response type
   * @param detail       detail failed message if the response is not success
   * @param partResult   result
   */
  public IndexPartGetRowsResponse(ResponseType responseType, String detail,
    IndexPartGetRowsResult partResult) {
    super(responseType, detail);
    this.partResult = partResult;
  }

  public IndexPartGetRowsResponse(ResponseType responseType, String detail) {
    this(responseType, detail, null);
  }

  public IndexPartGetRowsResponse(ResponseType responseType) {
    this(responseType, "", null);
  }

  public IndexPartGetRowsResponse() {
    this(ResponseType.SUCCESS);
  }

  public IndexPartGetRowsResult getPartResult() {
    return partResult;
  }

  public void setPartResult(IndexPartGetRowsResult partResult) {
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
          partResult = new IndexPartGetRowsDoubleResult();
          break;
        case FLOAT:
          partResult = new IndexPartGetRowsFloatResult();
          break;
        case INT:
          partResult = new IndexPartGetRowsIntResult();
          break;
        case LONG:
          partResult = new IndexPartGetRowsLongResult();
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
