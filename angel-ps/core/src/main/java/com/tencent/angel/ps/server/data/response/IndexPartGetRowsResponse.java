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

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.psagent.matrix.transport.router.ValuePart;
import io.netty.buffer.ByteBuf;

public class IndexPartGetRowsResponse extends ResponseData {
  private ValuePart[] valueParts;

  /**
   * Create a new GetUDFResponse
   *
   * @param valueParts   result
   */
  public IndexPartGetRowsResponse(ValuePart[] valueParts) {
    this.valueParts = valueParts;
  }

  public IndexPartGetRowsResponse() {
    this(null);
  }

  @Override public void serialize(ByteBuf buf) {
    ByteBufSerdeUtils.serializeInt(buf, valueParts.length);
    for(int i = 0; i < valueParts.length; i++) {
      ByteBufSerdeUtils.serializeValuePart(buf, valueParts[i]);
    }
  }

  @Override public void deserialize(ByteBuf buf) {
    valueParts = new ValuePart[ByteBufSerdeUtils.deserializeInt(buf)];
    for(int i = 0; i < valueParts.length; i++) {
      valueParts[i] = ByteBufSerdeUtils.deserializeValuePart(buf);
    }
  }

  @Override public int bufferLen() {
    int len = ByteBufSerdeUtils.INT_LENGTH;
    for(int i = 0; i < valueParts.length; i++) {
      len += ByteBufSerdeUtils.serializedValuePartLen(valueParts[i]);
    }
    return len;
  }

  public ValuePart[] getValueParts() {
    return valueParts;
  }

  public void setValueParts(ValuePart[] valueParts) {
    this.valueParts = valueParts;
  }
}
