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
package com.tencent.angel.graph.psf.hyperanf;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class GetClosenessPartResult extends PartitionGetResult {

  private Long2DoubleOpenHashMap closenesses;

  public GetClosenessPartResult(Long2DoubleOpenHashMap closenesses) {
    this.closenesses = closenesses;
  }

  public GetClosenessPartResult() {
    this.closenesses = new Long2DoubleOpenHashMap();
  }

  public Long2DoubleOpenHashMap getClosenesses() {
    return closenesses;
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeInt(closenesses.size());
    ObjectIterator<Long2DoubleMap.Entry> it =
        closenesses.long2DoubleEntrySet().fastIterator();
    while (it.hasNext()) {
      Long2DoubleMap.Entry entry = it.next();
      output.writeLong(entry.getLongKey());
      output.writeDouble(entry.getDoubleValue());
    }

  }

  @Override
  public void deserialize(ByteBuf input) {
    int size = input.readInt();
    closenesses = new Long2DoubleOpenHashMap();
    for (int i = 0; i < size; i++) {
      long key = input.readLong();
      double val = input.readDouble();
      closenesses.put(key, val);
    }
  }

  @Override
  public int bufferLen() {
    int len = 4;
    len += closenesses.size() * 16;
    return len;
  }
}
