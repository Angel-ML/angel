package com.tencent.angel.psagent.matrix.transport.router;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.RowType;
import io.netty.buffer.ByteBuf;

public abstract class KeyValuePart extends DataPart {
  private int rowId;

  public KeyValuePart(int rowId) {
    this.rowId = rowId;
  }

  public abstract RowType getKeyValueType();

  public abstract RouterType getRouterType();

  public boolean isComp() {
    return false;
  }

  public int getRowId() {
    return rowId;
  }

  public void setRowId(int rowId) {
    this.rowId = rowId;
  }

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
    return ByteBufSerdeUtils.INT_LENGTH * 2;
  }

}
