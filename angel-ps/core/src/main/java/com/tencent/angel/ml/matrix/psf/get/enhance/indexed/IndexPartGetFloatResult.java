package com.tencent.angel.ml.matrix.psf.get.enhance.indexed;

import com.tencent.angel.PartitionKey;
import io.netty.buffer.ByteBuf;

public class IndexPartGetFloatResult extends IndexPartGetResult {
  private float[] values;

  public IndexPartGetFloatResult(PartitionKey partKey, float [] values) {
    super(partKey);
    this.values = values;
  }

  public IndexPartGetFloatResult() {
    this(null, null);
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(values.length);
    for (int i = 0; i < values.length; i++) {
      buf.writeFloat(values[i]);
    }
  }

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int len = buf.readInt();
    values = new float[len];
    for (int i = 0; i < len; i++) {
      values[i] = buf.readFloat();
    }
  }

  @Override public int bufferLen() {
    return super.bufferLen() + 4 + ((values != null) ? values.length * 4 : 0);
  }

  public float[] getValues() {
    return values;
  }
}
