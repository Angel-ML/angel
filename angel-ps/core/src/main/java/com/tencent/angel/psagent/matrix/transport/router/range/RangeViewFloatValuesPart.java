package com.tencent.angel.psagent.matrix.transport.router.range;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.psagent.matrix.transport.router.operator.IIntKeyFloatValuePartOp;
import io.netty.buffer.ByteBuf;

public class RangeViewFloatValuesPart extends RangeKeyValuePart implements IIntKeyFloatValuePartOp {
  /**
   * Whole key array before serialization, sorted by asc
   */
  private int[] keys;

  /**
   * Whole value array before serialization
   */
  private float[] values;

  public RangeViewFloatValuesPart(int rowId, float[] values, int startPos, int endPos) {
    super(rowId, startPos, endPos);
    this.values = values;
  }

  public RangeViewFloatValuesPart() {
    this(-1, null, -1, -1);
  }

  @Override
  public RowType getKeyValueType() {
    return RowType.T_FLOAT_DENSE;
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
  public float[] getValues() {
    return values;
  }

  @Override
  public void add(int key, float value) {
    throw new UnsupportedOperationException("Unsupport dynamic add for range view part now");
  }

  @Override
  public void add(int[] keys, float[] values) {
    throw new UnsupportedOperationException("Unsupport dynamic add for range view part now");
  }
  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    ByteBufSerdeUtils.serializeFloats(output, values, startPos, endPos);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    values = ByteBufSerdeUtils.deserializeFloats(input);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedFloatsLen(values, startPos, endPos);
  }

}
