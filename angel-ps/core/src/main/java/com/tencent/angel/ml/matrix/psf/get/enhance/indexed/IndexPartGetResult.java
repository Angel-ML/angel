package com.tencent.angel.ml.matrix.psf.get.enhance.indexed;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

/**
 * Base class for partition index get row result
 */
public abstract class IndexPartGetResult extends PartitionGetResult {
  private PartitionKey partKey;

  public IndexPartGetResult(PartitionKey partKey) {
    this.partKey = partKey;
  }

  public IndexPartGetResult() {
    this(null);
  }

  @Override public void serialize(ByteBuf buf) {
    partKey.serialize(buf);
  }

  @Override public void deserialize(ByteBuf buf) {
    partKey = new PartitionKey();
    partKey.deserialize(buf);
  }

  @Override public int bufferLen() {
    return (partKey != null) ? partKey.bufferLen() : 0 ;
  }

  public PartitionKey getPartKey() {
    return partKey;
  }
}
