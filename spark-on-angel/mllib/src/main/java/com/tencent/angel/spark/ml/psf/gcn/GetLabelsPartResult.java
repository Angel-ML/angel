package com.tencent.angel.spark.ml.psf.gcn;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class GetLabelsPartResult extends PartitionGetResult {
  private long[] keys;
  private float[] values;

  public GetLabelsPartResult(long[] keys, float[] values) {
    this.keys = keys;
    this.values = values;
  }

  public GetLabelsPartResult() {}

  public long[] getKeys() {
    return keys;
  }

  public float[] getValues() {
    return values;
  }

  public int size() {
    return keys.length;
  }

  @Override
  public void serialize(ByteBuf output) {
    assert (keys.length == values.length);
    output.writeInt(keys.length);
    for (int i = 0; i < keys.length; i++)
      output.writeLong(keys[i]);
    for (int i = 0; i < values.length; i++)
      output.writeFloat(values[i]);
  }

  @Override
  public void deserialize(ByteBuf input) {
    int len = input.readInt();
    keys = new long[len];
    values = new float[len];
    for (int i = 0; i < len; i++)
      keys[i] = input.readLong();
    for (int i = 0; i < len; i++)
      values[i] = input.readFloat();
  }

  @Override
  public int bufferLen() {
    int len = 4;
    len += 4 * keys.length + 8 * values.length;
    return len;
  }

}
