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
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import scala.Tuple3;

public class GetClosenessAndCardinalityPartResult extends PartitionGetResult {

  private Long2ObjectOpenHashMap<Tuple3<Double, Long, Long>> closenesses;

  public GetClosenessAndCardinalityPartResult(Long2ObjectOpenHashMap<Tuple3<Double, Long, Long>> closenesses) {
    this.closenesses = closenesses;
  }

  public GetClosenessAndCardinalityPartResult() {
    this.closenesses = new Long2ObjectOpenHashMap<>();
  }

  public Long2ObjectOpenHashMap<Tuple3<Double, Long, Long>> getClosenesses() {
    return closenesses;
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeInt(closenesses.size());
    ObjectIterator<Long2ObjectMap.Entry<Tuple3<Double, Long, Long>>> it =
        closenesses.long2ObjectEntrySet().fastIterator();
    while (it.hasNext()) {
      Long2ObjectMap.Entry<Tuple3<Double, Long, Long>> entry = it.next();
      output.writeLong(entry.getLongKey());
      output.writeDouble(entry.getValue()._1());
      output.writeLong(entry.getValue()._2());
      output.writeLong(entry.getValue()._3());
    }

  }

  @Override
  public void deserialize(ByteBuf input) {
    int size = input.readInt();
    closenesses = new Long2ObjectOpenHashMap<>();
    for (int i = 0; i < size; i++) {
      long key = input.readLong();
      double centrality = input.readDouble();
      long cardinality = input.readLong();
      long disSum = input.readLong();
      closenesses.put(key, new Tuple3<>(centrality, cardinality, disSum));
    }
  }

  @Override
  public int bufferLen() {
    int len = 4;
    len += closenesses.size() * 32;
    return len;
  }
}
