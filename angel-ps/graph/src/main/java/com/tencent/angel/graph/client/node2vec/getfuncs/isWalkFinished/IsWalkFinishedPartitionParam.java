package com.tencent.angel.graph.client.node2vec.getfuncs.isWalkFinished;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class IsWalkFinishedPartitionParam extends PartitionGetParam {

  public IsWalkFinishedPartitionParam(int matrixId, PartitionKey partKey) {
    super(matrixId, partKey);
  }

  public IsWalkFinishedPartitionParam() {
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
    return super.bufferLen();
  }
}
