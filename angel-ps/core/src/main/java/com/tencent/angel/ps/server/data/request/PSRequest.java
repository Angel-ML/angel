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

import com.tencent.angel.ps.ParameterServerId;
import io.netty.buffer.ByteBuf;

public abstract class PSRequest extends Request {
  private ParameterServerId psId;
  /**
   * Create a new Request.
   *
   * @param userRequestId user request id
   */
  public PSRequest(int userRequestId, ParameterServerId psId) {
    super(userRequestId, new RequestContext());
    this.psId = psId;
  }

  public ParameterServerId getPsId() {
    return psId;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(psId.getIndex());
  }

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    psId = new ParameterServerId(buf.readInt());
  }

  @Override public int bufferLen() {
    return 4 + super.bufferLen();
  }
}
