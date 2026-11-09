package com.tencent.angel.graph.client.psf.universalembedding.adsEmbedding;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.graph.client.psf.universalembedding.PartUniversalEmbeddingInitParam;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import io.netty.buffer.ByteBuf;

public class PartUniversalEmbeddingWithFieldInitParam extends PartUniversalEmbeddingInitParam {

  private int fieldId;

  public PartUniversalEmbeddingWithFieldInitParam(int matrixId, PartitionKey partKey,
      KeyPart indicesPart, int fieldId,
      int seed, int dim, int numSlots, float[] floats, int[] ints) {
    super(matrixId, partKey, indicesPart, seed, dim, numSlots, floats, ints);
    this.fieldId = fieldId;
  }

  public PartUniversalEmbeddingWithFieldInitParam() {
    this(-1, null, null, -1, -1, -1, -1, null, null);
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
