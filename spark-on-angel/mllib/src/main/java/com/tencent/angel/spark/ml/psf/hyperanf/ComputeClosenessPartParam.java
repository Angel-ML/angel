package com.tencent.angel.spark.ml.psf.hyperanf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class ComputeClosenessPartParam extends PartitionUpdateParam {

  private int r;

  public ComputeClosenessPartParam(int matrixId, PartitionKey pkey, int r) {
    super(matrixId, pkey);
    this.r = r;
  }

  public ComputeClosenessPartParam() {
    this(0, null, 0);
  }

  public int getR() {
    return r;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(r);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    r = buf.readInt();
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 4;
    return len;
  }
}
