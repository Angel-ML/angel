package com.tencent.angel.graph.client.psf.universalembedding.adsEmbedding;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.graph.client.psf.universalembedding.PartUniversalEmbeddingExtraInitParam;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import io.netty.buffer.ByteBuf;

public class PartUniversalEmbeddingWithFieldExtraInitParam extends
    PartUniversalEmbeddingExtraInitParam {

  private int fieldId;

  public PartUniversalEmbeddingWithFieldExtraInitParam(int matrixId, PartitionKey partKey,
      KeyValuePart keyValuePart,
      int fieldId, int dim, int numSlots) {
    super(matrixId, partKey, keyValuePart, dim, numSlots);
    this.fieldId = fieldId;
  }

  public PartUniversalEmbeddingWithFieldExtraInitParam() {
    this(-1, null, null, -1, -1, -1);
  }

  public int getFieldId() {
    return fieldId;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    ByteBufSerdeUtils.serializeInt(buf, fieldId);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    fieldId = ByteBufSerdeUtils.deserializeInt(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.INT_LENGTH;
  }
}
