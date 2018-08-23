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


package com.tencent.angel.ml.lda.psf;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PartColumnResult extends PartitionGetResult {

  private ByteBuf buf;
  public Map<Integer, Int2IntOpenHashMap> cks;

  public PartColumnResult(Map<Integer, Int2IntOpenHashMap> cks) {
    this.cks = cks;
  }

  public PartColumnResult() {
  }

  @Override public void serialize(ByteBuf buf) {
    Iterator<Integer> keyIterator = cks.keySet().iterator();

    buf.writeInt(cks.size());

    while (keyIterator.hasNext()) {
      int column = keyIterator.next();
      buf.writeInt(column);
      Int2IntOpenHashMap ck = cks.get(column);
      buf.writeInt(ck.size());
      ObjectIterator<Int2IntMap.Entry> iter = ck.int2IntEntrySet().fastIterator();
      while (iter.hasNext()) {
        Int2IntMap.Entry entry = iter.next();
        buf.writeInt(entry.getIntKey());
        buf.writeInt(entry.getIntValue());
      }
    }
  }

  @Override public int bufferLen() {
    return 16;
  }

  @Override public void deserialize(ByteBuf buf) {
    int numColumns = buf.readInt();

    cks = new HashMap();
    for (int i = 0; i < numColumns; i++) {
      int column = buf.readInt();
      int size = buf.readInt();
      Int2IntOpenHashMap ck = new Int2IntOpenHashMap(size);
      for (int j = 0; j < size; j++) {
        ck.put(buf.readInt(), buf.readInt());
      }

      cks.put(column, ck);
    }
  }

  public void merge(PartColumnResult other) {
    Iterator<Integer> keyIterator = other.cks.keySet().iterator();
    while (keyIterator.hasNext()) {
      int column = keyIterator.next();
      if (cks.containsKey(column)) {
        cks.get(column).putAll(other.cks.get(column));
      } else {
        cks.put(column, other.cks.get(column));
      }
    }
  }
}
