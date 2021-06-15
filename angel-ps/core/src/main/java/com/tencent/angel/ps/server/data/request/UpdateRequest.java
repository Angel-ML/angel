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
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import io.netty.buffer.ByteBuf;

public class UpdateRequest extends RequestData implements IStreamRequest {
  /**
   * the update row splits
   */
  private KeyValuePart updatePart;

  /**
   * Update method
   */
  private UpdateOp op;

  /**
   * Input buffer, just used by stream mode
   */
  private ByteBuf in;

  /**
   * Create PutPartitionUpdateRequest.
   *
   * @param updatePart     update part
   */
  public UpdateRequest(KeyValuePart updatePart, UpdateOp op) {
    this.updatePart = updatePart;
    this.op = op;
  }

  public UpdateRequest() {
    this(null, UpdateOp.PLUS);
  }

  public UpdateOp getOp() {
    return op;
  }

  public void setOp(UpdateOp op) {
    this.op = op;
  }

  @Override public void serialize(ByteBuf buf) {
    buf.writeInt(op.getOpId());
    ByteBufSerdeUtils.serializeKeyValuePart(buf, updatePart);
  }

  @Override public void deserialize(ByteBuf buf) {
    int readerIndex = buf.readerIndex();
    op = UpdateOp.valueOf(buf.readInt());
    updatePart = ByteBufSerdeUtils.deserializeKeyValuePart(buf);
    requestSize = buf.readerIndex() - readerIndex;
  }

  @Override public int bufferLen() {
    return ByteBufSerdeUtils.INT_LENGTH + ByteBufSerdeUtils.serializedKeyValuePartLen(updatePart);
  }

  @Override public String toString() {
    return "PutPartitionUpdateRequest rowsSplit size=" + ((updatePart
      != null) ? updatePart.size() : 0) + ", toString()=" + super
      .toString() + "]";
  }

  @Override
  public void deserializeHeader(ByteBuf buf) {
    op = UpdateOp.valueOf(ByteBufSerdeUtils.deserializeInt(buf));
    updatePart = null;
    in = buf;
  }

  @Override
  public ByteBuf getInputBuffer() {
    return in;
  }

  public KeyValuePart getUpdatePart() {
    return updatePart;
  }

  public void setUpdatePart(KeyValuePart updatePart) {
    this.updatePart = updatePart;
  }
}
