package com.tencent.angel.ml.matrix.psf.get.enhance.indexed;

import com.tencent.angel.PartitionKey;
import io.netty.buffer.ByteBuf;

public class IndexPartGetIntResult extends IndexPartGetResult {
  private int[] values;

  public IndexPartGetIntResult(PartitionKey partKey, int [] values) {
    super(partKey);
    this.values = values;
  }

  public IndexPartGetIntResult() {
    this(null, null);
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(values.length);
    for (int i = 0; i < values.length; i++) {
      buf.writeInt(values[i]);
    }
  }

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int len = buf.readInt();
    values = new int[len];
    for (int i = 0; i < len; i++) {
      values[i] = buf.readInt();
    }
  }

  @Override public int bufferLen() {
    return super.bufferLen() + 4 + ((values != null) ? values.length * 4 : 0);
  }

  public int[] getValues() {
    return values;
  }
}
