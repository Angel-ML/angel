package com.tencent.angel.psagent.matrix.transport.router.value;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ps.server.data.request.ValueType;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.ValuePart;
import com.tencent.angel.psagent.matrix.transport.router.operator.IAnyValuePartOp;
import io.netty.buffer.ByteBuf;

public class AnyValuesPart extends ValuePart implements IAnyValuePartOp {
  private IElement[] values;

  public AnyValuesPart(int rowId, IElement[] values) {
    super(rowId);
    this.values = values;
  }

  public AnyValuesPart(IElement[] values) {
    this(-1, values);
  }

  public AnyValuesPart() {
    this(-1, null);
  }

  @Override
  public ValueType getValueType() {
    return ValueType.ANY;
  }

  @Override
  public int size() {
    return values.length;
  }

  @Override
  public IElement[] getValues() {
    return values;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    ByteBufSerdeUtils.serializeObjects(output, values);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    values = ByteBufSerdeUtils.deserializeObjects(input);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedObjectsLen(values);
  }
}
