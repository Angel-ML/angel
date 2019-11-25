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
package com.tencent.angel.graph.client.node2vec.updatefuncs.pushpathtail;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.client.node2vec.params.PartitionUpdateParamWithIds;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

public class PushPathTailPartitionParam extends PartitionUpdateParamWithIds {
  private Long2LongOpenHashMap pathTail;

  public PushPathTailPartitionParam(int matrixId, PartitionKey partKey,
                                    Long2LongOpenHashMap pathTail,
                                    long[] nodeIds, int startIndex, int endIndex) {
    super(matrixId, partKey, nodeIds, startIndex, endIndex);
    this.pathTail = pathTail;
  }

  public PushPathTailPartitionParam() {
    super();
  }

  public Long2LongOpenHashMap getPathTail() {
    return pathTail;
  }

  public void setPathTail(Long2LongOpenHashMap pathTail) {
    this.pathTail = pathTail;
  }

  @Override
  protected void clear() {
    super.clear();
    pathTail = null;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    for (int i = startIdx; i < endIdx; i++) {
      long key = keyIds[i];
      buf.writeLong(key);
      buf.writeLong(pathTail.get(key));
    }
    clear();
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int size = endIdx - startIdx;
    pathTail = new Long2LongOpenHashMap(size);

    for (int i = 0; i < size; i++) {
      long key = buf.readLong();
      long value = buf.readLong();
      pathTail.put(key, value);
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 16 * (endIdx - startIdx);
    return len;
  }

}
