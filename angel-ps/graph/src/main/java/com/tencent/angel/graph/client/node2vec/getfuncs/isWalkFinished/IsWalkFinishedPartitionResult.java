package com.tencent.angel.graph.client.node2vec.getfuncs.isWalkFinished;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class IsWalkFinishedPartitionResult extends PartitionGetResult {

  private long count;

  public IsWalkFinishedPartitionResult(long count) {
    this.count = count;
  }

  public IsWalkFinishedPartitionResult() {
    super();
  }

  public boolean isWalkFinished() {
    return count == 0;
  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeLong(count);
  }

  @Override
  public void deserialize(ByteBuf input) {
    count = input.readLong();
  }

  @Override
  public int bufferLen() {
    return 8;
  }
}
