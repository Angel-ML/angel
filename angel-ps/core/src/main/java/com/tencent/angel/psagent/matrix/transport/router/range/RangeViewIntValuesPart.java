package com.tencent.angel.psagent.matrix.transport.router.range;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.psagent.matrix.transport.router.operator.IIntKeyIntValuePartOp;
import io.netty.buffer.ByteBuf;

public class RangeViewIntValuesPart extends RangeKeyValuePart implements IIntKeyIntValuePartOp {
  /**
   * Whole key array before serialization, sorted by asc
   */
  private int[] keys;

  /**
   * Whole value array before serialization
   */
  private int[] values;


  public RangeViewIntValuesPart(int rowId, int[] values, int startPos, int endPos) {
    super(rowId, startPos, endPos);
    this.values = values;
  }

  public RangeViewIntValuesPart(int[] values, int startPos, int endPos) {
    this(-1, values, startPos, endPos);
  }

  public RangeViewIntValuesPart() {
    this(-1, null, -1, -1);
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
  public int[] getValues() {
    return values;
  }

  @Override
  public void add(int key, int value) {
    throw new UnsupportedOperationException("Unsupport dynamic add for range view part now");
  }

  @Override
  public void add(int[] keys, int[] values) {
    throw new UnsupportedOperationException("Unsupport dynamic add for range view part now");
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
  public RowType getKeyValueType() {
    return RowType.T_INT_DENSE;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    ByteBufSerdeUtils.serializeInts(output, values, startPos, endPos);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    values = ByteBufSerdeUtils.deserializeInts(input);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedIntsLen(values, startPos, endPos);
  }
}
