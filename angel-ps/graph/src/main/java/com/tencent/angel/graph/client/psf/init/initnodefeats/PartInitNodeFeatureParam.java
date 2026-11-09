package com.tencent.angel.graph.client.psf.init.initnodefeats;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.graph.data.FeatureKeyType;
import com.tencent.angel.ml.matrix.psf.update.base.GeneralPartUpdateParam;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import io.netty.buffer.ByteBuf;

public class PartInitNodeFeatureParam extends GeneralPartUpdateParam {

  public FeatureKeyType keyType;

  public PartInitNodeFeatureParam(int matrixId, PartitionKey partKey, KeyValuePart keyValuePart, FeatureKeyType keyType) {
    super(matrixId, partKey, keyValuePart);
    this.keyType = keyType;
  }

  public PartInitNodeFeatureParam() {
    this(-1, null, null, FeatureKeyType.INT);
  }

  public FeatureKeyType getKeyType() {
    return keyType;
  }

  public void setKeyType(FeatureKeyType keyType) {
    this.keyType = keyType;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    ByteBufSerdeUtils.serializeByte(buf, keyType.getIndex());
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    keyType = FeatureKeyType.valueOf(ByteBufSerdeUtils.deserializeByte(buf));
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.BYTE_LENGTH;
  }
}
