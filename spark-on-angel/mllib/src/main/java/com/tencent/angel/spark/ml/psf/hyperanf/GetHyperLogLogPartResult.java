package com.tencent.angel.spark.ml.psf.hyperanf;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.IOException;

public class GetHyperLogLogPartResult extends PartitionGetResult {

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
      e.printStackTrace();
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
      e.printStackTrace();
    }
  }

  @Override
  public int bufferLen() {
    int len = 4;
    ObjectIterator<Long2ObjectMap.Entry<HyperLogLogPlus>> it =
      logs.long2ObjectEntrySet().fastIterator();
    while (it.hasNext()) {
      Long2ObjectMap.Entry<HyperLogLogPlus> entry = it.next();
      len += 8;
      len += 4;
      len += entry.getValue().sizeof();
    }
    return len;
  }
}
