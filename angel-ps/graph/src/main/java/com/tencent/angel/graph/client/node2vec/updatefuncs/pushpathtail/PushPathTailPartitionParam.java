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
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

public class PushPathTailPartitionParam extends PartitionUpdateParam {

  private long[] keyIds;
  private int startIdx;
  private int endIdx;
  private Long2LongOpenHashMap pathTail;


  public PushPathTailPartitionParam(int matrixId, PartitionKey partKey,
      Long2LongOpenHashMap pathTail,
      long[] nodeIds, int startIndex, int endIndex) {
    super(matrixId, partKey, false);
    this.keyIds = nodeIds;
    this.startIdx = startIndex;
    this.endIdx = endIndex;
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

  protected void clear() {
    this.keyIds = null;
    this.startIdx = 0;
    this.endIdx = 0;
    this.pathTail = null;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    int size = endIdx - startIdx;
    buf.writeInt(size);
    // System.out.println(Thread.currentThread().getId() + "\t serialize -> " + ());
    for (int i = startIdx; i < endIdx; i++) {
      long key = keyIds[i];
      buf.writeLong(key);
      buf.writeLong(pathTail.get(key));
    }
    //clear();
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    this.startIdx = 0;
    this.endIdx = buf.readInt();
    int size = endIdx - startIdx;
    // System.out.println(Thread.currentThread().getId() + "\t deserialize -> " + size);
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
    len += 16 * (endIdx - startIdx) + 4;
    return len;
  }

}
