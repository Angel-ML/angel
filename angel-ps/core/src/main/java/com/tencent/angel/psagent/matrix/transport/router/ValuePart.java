package com.tencent.angel.psagent.matrix.transport.router;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ps.server.data.request.ValueType;
import io.netty.buffer.ByteBuf;

public abstract class ValuePart extends DataPart {
  private int rowId;

  public ValuePart(int rowId) {
    this.rowId = rowId;
  }

  public abstract ValueType getValueType();

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
