package com.tencent.angel.graph.client.psf.universalembedding.adsEmbedding;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import io.netty.buffer.ByteBuf;

public class PartUniversalEmbeddingWithFieldGetParam extends PartitionGetParam {

  private KeyValuePart keyValuePart;

  public PartUniversalEmbeddingWithFieldGetParam(int matrixId, PartitionKey part,
      KeyValuePart keyValuePart) {
    super(matrixId, part);
    this.keyValuePart = keyValuePart;
  }

  public PartUniversalEmbeddingWithFieldGetParam() {
    this(-1, null, null);
  }

  public KeyValuePart getKeyValuePart() {
    return keyValuePart;
  }

  public void setKeyValuePart(KeyValuePart keyValuePart) {
    this.keyValuePart = keyValuePart;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    ByteBufSerdeUtils.serializeKeyValuePart(buf, keyValuePart);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    keyValuePart = ByteBufSerdeUtils.deserializeKeyValuePart(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedKeyValuePartLen(keyValuePart);
  }
}
