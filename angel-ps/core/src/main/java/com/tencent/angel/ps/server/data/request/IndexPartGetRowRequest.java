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

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import io.netty.buffer.ByteBuf;

public class IndexPartGetRowRequest extends RequestData implements IStreamRequest {
  private int rowId;
  private KeyPart keyPart;
  private InitFunc func;
  private ByteBuf in;

  public IndexPartGetRowRequest(int rowId, KeyPart indexPart, InitFunc func) {
    this.rowId = rowId;
    this.keyPart = indexPart;
    this.func = func;
  }

  public IndexPartGetRowRequest() {
    this(-1, null, null);
  }

  public int getRowId() {
    return rowId;
  }

  public KeyPart getKeyPart() {
    return keyPart;
  }

  public InitFunc getFunc() {
    return func;
  }

  @Override public void serialize(ByteBuf out) {
    ByteBufSerdeUtils.serializeInt(out, rowId);

    ByteBufSerdeUtils.serializeBoolean(out, func != null);
    if(func != null) {
      ByteBufSerdeUtils.serializeObject(out, func);
    }

    ByteBufSerdeUtils.serializeKeyPart(out, keyPart);
  }

  @Override public void deserialize(ByteBuf in) {
    int readerIndex = in.readerIndex();
    rowId = ByteBufSerdeUtils.deserializeInt(in);

    boolean useInitFunc = ByteBufSerdeUtils.deserializeBoolean(in);
    if(useInitFunc) {
      func = (InitFunc) ByteBufSerdeUtils.deserializeObject(in);
    }

    keyPart = ByteBufSerdeUtils.deserializeKeyPart(in);
    requestSize = in.readerIndex() - readerIndex;
  }

  @Override public int bufferLen() {
    int len = ByteBufSerdeUtils.serializedIntLen(rowId);

    len += ByteBufSerdeUtils.serializedBooleanLen(func != null);
    if(func != null) {
      len += ByteBufSerdeUtils.serializedObjectLen(func);
    }

    len += ByteBufSerdeUtils.serializedKeyPartLen(keyPart);
    return len;
  }

  @Override
  public void deserializeHeader(ByteBuf in) {
    rowId = ByteBufSerdeUtils.deserializeInt(in);

    boolean useInitFunc = ByteBufSerdeUtils.deserializeBoolean(in);
    if(useInitFunc) {
      func = (InitFunc) ByteBufSerdeUtils.deserializeObject(in);
    }

    // Data is not de-serialized first
    keyPart = null;
    this.in = in;
  }

  @Override
  public ByteBuf getInputBuffer() {
    return in;
  }

  public ByteBuf getIn() {
    return in;
  }

  public void setIn(ByteBuf in) {
    this.in = in;
  }
}
