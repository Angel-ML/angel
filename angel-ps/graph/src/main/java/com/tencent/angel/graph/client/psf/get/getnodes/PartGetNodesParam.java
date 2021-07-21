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

package com.tencent.angel.graph.client.psf.get.getnodes;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PartGetNodesParam extends PartitionGetParam {

  private Boolean isHash;

  public PartGetNodesParam() {
    this(0, null, true);
  }

  public PartGetNodesParam(int matrixId, PartitionKey partKey, Boolean isHash) {
    super(matrixId, partKey);
    this.isHash = isHash;
  }

  public Boolean getHash() {
    return isHash;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    ByteBufSerdeUtils.serializeBoolean(buf, isHash);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    isHash = ByteBufSerdeUtils.deserializeBoolean(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedBooleanLen(isHash);
  }
}
