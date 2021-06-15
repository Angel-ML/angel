package com.tencent.angel.psagent.matrix.transport.router.value;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ps.server.data.request.ValueType;
import com.tencent.angel.psagent.matrix.transport.router.ValuePart;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongValuePartOp;
import io.netty.buffer.ByteBuf;

public class LongValuesPart extends ValuePart implements ILongValuePartOp {
  private long[] values;

  public LongValuesPart(int rowId, long[] values) {
    super(rowId);
    this.values = values;
  }

  public LongValuesPart(long[] values) {
    this(-1, values);
  }

  public LongValuesPart() {
    this(-1, null);
  }

  @Override
  public ValueType getValueType() {
    return ValueType.LONG;
  }

  @Override
  public int size() {
    return values.length;
  }

  @Override
  public long[] getValues() {
    return values;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    ByteBufSerdeUtils.serializeLongs(output, values);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    values = ByteBufSerdeUtils.deserializeLongs(input);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedLongsLen(values);
  }
}
