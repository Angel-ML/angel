package com.tencent.angel.ml.matrix.psf.update.base;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import io.netty.buffer.ByteBuf;

/**
 * General partition update parameter
 */
public class GeneralPartUpdateParam extends PartitionUpdateParam {

  /**
   * Key and value data partition
   */
  private KeyValuePart keyValuePart;

  public GeneralPartUpdateParam(int matrixId, PartitionKey partKey, KeyValuePart keyValuePart) {
    super(matrixId, partKey);
    this.keyValuePart = keyValuePart;
  }

  public GeneralPartUpdateParam() {
    this(-1, null, null);
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

  public KeyValuePart getKeyValuePart() {
    return keyValuePart;
  }

  public void setKeyValuePart(KeyValuePart keyValuePart) {
    this.keyValuePart = keyValuePart;
  }
}
