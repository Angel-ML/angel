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
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.utils.StringUtils;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GetHyperLogLogPartResult extends PartitionGetResult {

  private static final Log LOG = LogFactory.getLog(UpdateHyperLogLogPartParam.class);

  private Long2ObjectOpenHashMap<HyperLogLogPlus> logs;

  public GetHyperLogLogPartResult(Long2ObjectOpenHashMap<HyperLogLogPlus> logs) {
    this.logs = logs;
  }

  public GetHyperLogLogPartResult() {
    this.logs = new Long2ObjectOpenHashMap<>();
  }

  public Long2ObjectOpenHashMap<HyperLogLogPlus> getLogs() {
    return logs;
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeInt(logs.size());
    ObjectIterator<Long2ObjectMap.Entry<HyperLogLogPlus>> it =
            logs.long2ObjectEntrySet().fastIterator();
    try {
      while (it.hasNext()) {
        Long2ObjectMap.Entry<HyperLogLogPlus> entry = it.next();
        output.writeLong(entry.getLongKey());
        byte[] bytes = entry.getValue().getBytes();
        output.writeInt(bytes.length);
        output.writeBytes(bytes);
      }
    } catch (IOException e) {
      LOG.error("Serialize failed, details = " + StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    int size = input.readInt();
    logs = new Long2ObjectOpenHashMap<>();
    try {
      for (int i = 0; i < size; i++) {
        long key = input.readLong();
        int len = input.readInt();
        byte[] bytes = new byte[len];
        input.readBytes(bytes);
        HyperLogLogPlus plus = HyperLogLogPlus.Builder.build(bytes);
        logs.put(key, plus);
      }
    } catch (IOException e) {
      LOG.error("Deserialize failed, details = " + StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
  }

  @Override
  public int bufferLen() {
    int len = 4;
    ObjectIterator<Long2ObjectMap.Entry<HyperLogLogPlus>> it =
            logs.long2ObjectEntrySet().fastIterator();
    try {
      while (it.hasNext()) {
        Long2ObjectMap.Entry<HyperLogLogPlus> entry = it.next();
        len += 8;
        len += 4;
        len += entry.getValue().getBytes().length;
      }
    } catch (IOException e) {
      LOG.error("calc bufferLen failed, details = " + StringUtils.stringifyException(e));
      throw new RuntimeException(e);
    }
    return len;
  }
}
