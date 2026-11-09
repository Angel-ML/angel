package com.tencent.angel.graph.client.psf.get.getnodes;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PartGetNodesParam extends PartitionGetParam {

  private Boolean isHash;

  public PartGetNodesParam() {
    this(0, null, true);
  }

  public PartGetNodesParam(int matrixId, PartitionKey partKey, Boolean isHash) {
    super(matrixId, partKey);
    this.isHash = isHash;
  }

  public Boolean getHash() {
    return isHash;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    ByteBufSerdeUtils.serializeBoolean(buf, isHash);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    isHash = ByteBufSerdeUtils.deserializeBoolean(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedBooleanLen(isHash);
  }
}
