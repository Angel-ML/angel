package com.tencent.angel.graph.client.psf.sample.samplenodefeats;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PartSampleNodeFeatParam extends PartitionGetParam {

  /**
   * Sample number, if count <= 0, means return all neighbors
   */
  private int count;

  public PartSampleNodeFeatParam(int matrixId, PartitionKey part, int count) {
    super(matrixId, part);
    this.count = count;
  }

  public PartSampleNodeFeatParam() {
    this(0, null, 0);
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    ByteBufSerdeUtils.serializeInt(buf, count);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    count = ByteBufSerdeUtils.deserializeInt(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.INT_LENGTH;
  }
}
