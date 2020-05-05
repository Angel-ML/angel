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

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.io.IOException;

public class UpdateHyperLogLogPartParam extends PartitionUpdateParam {
  private long[] nodes;
  private int startIndex;
  private int endIndex;
  private Long2ObjectOpenHashMap<HyperLogLogPlus> updates;
  private int p;
  private int sp;

  public UpdateHyperLogLogPartParam(int matrixId, PartitionKey pkey,
                                    Long2ObjectOpenHashMap<HyperLogLogPlus> updates,
                                    int p, int sp,
                                    long[] nodes, int startIndex,
                                    int endIndex) {
    super(matrixId, pkey);
    this.updates = updates;
    this.nodes = nodes;
    this.p = p;
    this.sp = sp;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public UpdateHyperLogLogPartParam() {
    this(0, null, null, 0, 0, null, 0, 0);
  }

  public Long2ObjectOpenHashMap<HyperLogLogPlus> getUpdates() {
    return updates;
  }

  public long[] getNodes() {return nodes;}

  public int getSp() {return sp;}

  public int getP() {return p;}

  public void clear() {
    updates = null;
    nodes = null;
    startIndex = -1;
    endIndex = -1;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(p);
    buf.writeInt(sp);
    buf.writeInt(endIndex - startIndex);
    try {
      for (int i = startIndex; i < endIndex; i++) {
        long node = nodes[i];
        HyperLogLogPlus plus = updates.get(node);
        buf.writeLong(node);
        byte[] bytes = plus.getBytes();
        buf.writeInt(bytes.length);
        buf.writeBytes(bytes);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    clear();
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    p = buf.readInt();
    sp = buf.readInt();
    int size = buf.readInt();
    updates = new Long2ObjectOpenHashMap<>(size);

    try {
      for (int i = 0; i < size; i++) {
        long node = buf.readLong();
        int len = buf.readInt();
        byte[] bytes = new byte[len];
        buf.readBytes(bytes);
        updates.put(node, HyperLogLogPlus.Builder.build(bytes));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
