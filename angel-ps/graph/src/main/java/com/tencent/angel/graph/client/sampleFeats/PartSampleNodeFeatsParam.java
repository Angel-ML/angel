package com.tencent.angel.graph.client.sampleFeats;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PartSampleNodeFeatsParam extends PartitionGetParam {

  private int size;

  public PartSampleNodeFeatsParam(int matrixId, PartitionKey part, int size) {
    super(matrixId, part);
    this.size = size;
  }

  public PartSampleNodeFeatsParam() {
    this(-1, null, 0);
  }

  public int getSize() {
    return size;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(size);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    size = buf.readInt();
  }
}
