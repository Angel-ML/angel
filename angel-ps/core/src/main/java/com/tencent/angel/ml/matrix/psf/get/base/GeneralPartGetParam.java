package com.tencent.angel.ml.matrix.psf.get.base;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import io.netty.buffer.ByteBuf;

/**
 * General partition get parameter
 */
public class GeneralPartGetParam extends PartitionGetParam {

  /**
   * Indices partition
   */
  protected KeyPart indicesPart;

  public GeneralPartGetParam(int matrixId, PartitionKey partKey, KeyPart indicesPart) {
    super(matrixId, partKey);
    this.indicesPart = indicesPart;
  }

  public GeneralPartGetParam() {
    this(-1, null, null);
  }

  public KeyPart getIndicesPart() {
    return indicesPart;
  }

  public void setIndicesPart(KeyPart indicesPart) {
    this.indicesPart = indicesPart;
  }

  public int getRowId() {
    return this.indicesPart.getRowId();
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    ByteBufSerdeUtils.serializeKeyPart(buf, indicesPart);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    indicesPart = ByteBufSerdeUtils.deserializeKeyPart(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedKeyPartLen(indicesPart);
  }

}
