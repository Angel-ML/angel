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
package com.tencent.angel.graph.client.node2vec.params;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PartitionGetParamWithIds extends PartitionGetParam {
  protected long[] keyIds;
  protected int startIdx;
  protected int endIdx;

  public PartitionGetParamWithIds(int matrixId, PartitionKey partKey, long[] keyIds, int startIdx, int endIdx) {
    super(matrixId, partKey);
    this.keyIds = keyIds;
    this.startIdx = startIdx;
    this.endIdx = endIdx;
  }

  public PartitionGetParamWithIds() {
    super();
  }

  public long[] getKeyIds() {
    return keyIds;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    int length = endIdx - startIdx;
    buf.writeInt(length);

    for (int i = startIdx; i < endIdx; i++) {
      buf.writeLong(keyIds[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int length = buf.readInt();
    this.keyIds = new long[length];
    this.startIdx = 0;
    this.endIdx = length;

    for (int i = startIdx; i < endIdx; i++) {
      keyIds[i] = buf.readLong();
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    int length = endIdx - startIdx;
    return len + 4 + 8 * length;
  }
}
