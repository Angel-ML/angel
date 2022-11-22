package com.tencent.angel.graph.psf.hyperanf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class UpdateReadCounterPartParam extends PartitionUpdateParam {

  public UpdateReadCounterPartParam(int matrixId, PartitionKey pkey) {
    super(matrixId, pkey);
  }

  public UpdateReadCounterPartParam() {
    this(0, null);
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    return len;
  }
}