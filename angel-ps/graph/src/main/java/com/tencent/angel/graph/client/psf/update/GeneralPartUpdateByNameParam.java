package com.tencent.angel.graph.client.psf.update;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.update.base.GeneralPartUpdateParam;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import io.netty.buffer.ByteBuf;

public class GeneralPartUpdateByNameParam extends GeneralPartUpdateParam {

  /**
   * update by name
   */
  private String name;

  public GeneralPartUpdateByNameParam(int matrixId, PartitionKey partKey, KeyValuePart keyValuePart,
                                      String name) {
    super(matrixId, partKey, keyValuePart);
    this.name = name;
  }

  public GeneralPartUpdateByNameParam() {
    this(-1, null, null, null);
  }

  public String getName() {
    return name;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    ByteBufSerdeUtils.serializeBytes(buf, name.getBytes());
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    name = new String(ByteBufSerdeUtils.deserializeBytes(buf));
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedBytesLen(name.getBytes());
  }
}