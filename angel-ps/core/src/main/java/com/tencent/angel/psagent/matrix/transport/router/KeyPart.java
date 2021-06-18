package com.tencent.angel.psagent.matrix.transport.router;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ps.server.data.request.KeyType;
import io.netty.buffer.ByteBuf;

public abstract class KeyPart extends DataPart {
  private int rowId;
  public KeyPart(int rowId) {
    this.rowId = rowId;
  }

  public int getRowId() {
    return rowId;
  }

  public abstract KeyType getKeyType();

  public abstract RouterType getRouterType();

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeInt(output, rowId);
  }

  @Override
  public void deserialize(ByteBuf input) {
    rowId = ByteBufSerdeUtils.deserializeInt(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.INT_LENGTH;
  }
}
