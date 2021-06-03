package com.tencent.angel.psagent.matrix.transport.router.value;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ps.server.data.request.ValueType;
import com.tencent.angel.psagent.matrix.transport.router.ValuePart;
import com.tencent.angel.psagent.matrix.transport.router.operator.IFloatValuePartOp;
import io.netty.buffer.ByteBuf;

public class FloatValuesPart extends ValuePart implements IFloatValuePartOp {
  private float[] values;

  public FloatValuesPart(int rowId, float[] values) {
    super(rowId);
    this.values = values;
  }

  public FloatValuesPart(float[] values) {
    this(-1, values);
  }

  public FloatValuesPart() {
    this(-1, null);
  }

  @Override
  public ValueType getValueType() {
    return ValueType.FLOAT;
  }

  @Override
  public int size() {
    return values.length;
  }

  @Override
  public float[] getValues() {
    return values;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    ByteBufSerdeUtils.serializeFloats(output, values);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    values = ByteBufSerdeUtils.deserializeFloats(input);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedFloatsLen(values);
  }
}
