package com.tencent.angel.psagent.matrix.transport.router.value;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ps.server.data.request.ValueType;
import com.tencent.angel.psagent.matrix.transport.router.ValuePart;
import com.tencent.angel.psagent.matrix.transport.router.operator.IDoubleValuePartOp;
import io.netty.buffer.ByteBuf;

public class DoubleValuesPart extends ValuePart implements IDoubleValuePartOp {
  private double[] values;

  public DoubleValuesPart(int rowId, double[] values) {
    super(rowId);
    this.values = values;
  }

  public DoubleValuesPart(double[] values) {
    this(-1, values);
  }

  public DoubleValuesPart() {
    this(-1, null);
  }

  @Override
  public ValueType getValueType() {
    return ValueType.DOUBLE;
  }

  @Override
  public int size() {
    return values.length;
  }

  @Override
  public double[] getValues() {
    return values;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    ByteBufSerdeUtils.serializeDoubles(output, values);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    values = ByteBufSerdeUtils.deserializeDoubles(input);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedDoublesLen(values);
  }
}
