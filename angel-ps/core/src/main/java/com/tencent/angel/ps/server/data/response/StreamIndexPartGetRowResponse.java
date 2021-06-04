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

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.ValuePart;
import io.netty.buffer.ByteBuf;

public class StreamIndexPartGetRowResponse extends ResponseData implements IStreamResponse {
  /**
   * Result from PS
   */
  private ValuePart valuePart;

  /**
   * Partition key
   */
  private transient PartitionKey partKey;

  /**
   * Column indices, use by client
   */
  private transient KeyPart keyPart;

  /**
   * Use by stream rpc
   */
  private transient ByteBuf out;

  /**
   * Create a new StreamIndexPartGetRowResponse
   *
   * @param valuePart   result
   */
  public StreamIndexPartGetRowResponse(ValuePart valuePart) {
    this.valuePart = valuePart;
  }

  public StreamIndexPartGetRowResponse() {
    this(null);
  }

  @Override public void serialize(ByteBuf out) {
    ByteBufSerdeUtils.serializeValuePart(out, valuePart);
  }

  @Override public void deserialize(ByteBuf in) {
    valuePart = ByteBufSerdeUtils.deserializeValuePart(in);
  }

  @Override public int bufferLen() {
    return ByteBufSerdeUtils.serializedValuePartLen(valuePart);
  }

  @Override
  public ByteBuf getOutputBuffer() {
    return out;
  }

  @Override
  public void setOutputBuffer(ByteBuf out) {
    this.out = out;
  }

  public ValuePart getValuePart() {
    return valuePart;
  }

  public void setValuePart(ValuePart valuePart) {
    this.valuePart = valuePart;
  }

  public PartitionKey getPartKey() {
    return partKey;
  }

  public void setPartKey(PartitionKey partKey) {
    this.partKey = partKey;
  }

  public KeyPart getKeyPart() {
    return keyPart;
  }

  public void setKeyPart(KeyPart keyPart) {
    this.keyPart = keyPart;
  }

  public ByteBuf getOut() {
    return out;
  }

  public void setOut(ByteBuf out) {
    this.out = out;
  }
}
