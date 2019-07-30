package com.tencent.angel.spark.ml.psf.hyperanf;

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
