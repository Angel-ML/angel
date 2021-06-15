package com.tencent.angel.psagent.matrix.transport.router.value;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ps.server.data.request.ValueType;
import com.tencent.angel.psagent.matrix.transport.router.ValuePart;
import com.tencent.angel.psagent.matrix.transport.router.operator.IIntValuePartOp;
import io.netty.buffer.ByteBuf;

public class IntValuesPart extends ValuePart implements IIntValuePartOp {
  private int[] values;

  public IntValuesPart(int rowId, int[] values) {
    super(rowId);
    this.values = values;
  }

  public IntValuesPart(int[] values) {
    this(-1, values);
  }

  public IntValuesPart() {
    this(-1, null);
  }

  @Override
  public ValueType getValueType() {
    return ValueType.INT;
  }

  @Override
  public int size() {
    return values.length;
  }

  @Override
  public int[] getValues() {
    return values;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    ByteBufSerdeUtils.serializeInts(output, values);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    values = ByteBufSerdeUtils.deserializeInts(input);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedIntsLen(values);
  }
}
