package com.tencent.angel.psagent.matrix.transport.router.range;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.operator.IIntKeyAnyValuePartOp;
import io.netty.buffer.ByteBuf;

public class RangeViewAnyValuesPart extends RangeKeyValuePart implements IIntKeyAnyValuePartOp {
  /**
   * Whole key array before serialization, sorted by asc
   */
  private int[] keys;

  /**
   * Whole value array before serialization
   */
  private IElement[] values;

  public RangeViewAnyValuesPart(int rowId, IElement[] values, int startPos, int endPos) {
    super(rowId, startPos, endPos);
    this.values = values;
  }

  @Override
  public RowType getKeyValueType() {
    return RowType.T_ANY_INTKEY_DENSE;
  }

  @Override
  public int size() {
    if(endPos != -1) {
      return endPos - startPos;
    } else {
      return keys.length;
    }
  }

  @Override
  public int[] getKeys() {
    if(keys == null) {
      keys = new int[values.length];
      for(int i = 0; i < keys.length; i++) {
        keys[i] = i;
      }
    }

    return keys;
  }

  @Override
  public IElement[] getValues() {
    return values;
  }

  @Override
  public void add(int key, IElement value) {
    throw new UnsupportedOperationException("Unsupport dynamic add for range view part now");
  }

  @Override
  public void add(int[] keys, IElement[] values) {
    throw new UnsupportedOperationException("Unsupport dynamic add for range view part now");
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    ByteBufSerdeUtils.serializeObjects(output, values, startPos, endPos);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    values = ByteBufSerdeUtils.deserializeObjects(input);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedObjectsLen(values, startPos, endPos);
  }
}
