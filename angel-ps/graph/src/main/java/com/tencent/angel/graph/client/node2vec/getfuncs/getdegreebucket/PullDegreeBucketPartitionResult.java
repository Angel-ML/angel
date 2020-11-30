package com.tencent.angel.graph.client.node2vec.getfuncs.getdegreebucket;

import com.tencent.angel.graph.client.node2vec.utils.SerDe;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

public class PullDegreeBucketPartitionResult extends PartitionGetResult {
  private Int2IntOpenHashMap partResult;

  public PullDegreeBucketPartitionResult(Int2IntOpenHashMap partResult) {
    this.partResult = partResult;
  }

  public PullDegreeBucketPartitionResult() {
    super();
  }

  public Int2IntOpenHashMap getPartResult() {
    return partResult;
  }

  public void setPartResult(Int2IntOpenHashMap partResult) {
    this.partResult = partResult;
  }

  @Override
  public void serialize(ByteBuf output) {
    SerDe.serInt2IntMap(partResult, output);
  }

  @Override
  public void deserialize(ByteBuf input) {
    partResult = SerDe.deserInt2IntMap(input);
  }

  @Override
  public int bufferLen() {
    return 4 + partResult.size() * 8;
  }
}
