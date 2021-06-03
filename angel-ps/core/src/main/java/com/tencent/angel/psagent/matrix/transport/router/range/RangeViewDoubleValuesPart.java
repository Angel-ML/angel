package com.tencent.angel.psagent.matrix.transport.router.range;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.psagent.matrix.transport.router.operator.IIntKeyDoubleValuePartOp;
import io.netty.buffer.ByteBuf;

public class RangeViewDoubleValuesPart extends RangeKeyValuePart implements IIntKeyDoubleValuePartOp {
  /**
   * Whole key array before serialization, sorted by asc
   */
  private int[] keys;

  /**
   * Whole value array before serialization
   */
  private double[] values;


  public RangeViewDoubleValuesPart(int rowId, double[] values, int startPos, int endPos) {
    super(rowId, startPos, endPos);
    this.values = values;
  }

  public RangeViewDoubleValuesPart(double[] values, int startPos, int endPos) {
    this(-1, values, startPos, endPos);
  }

  public RangeViewDoubleValuesPart() {
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
  public double[] getValues() {
    return values;
  }

  @Override
  public void add(int key, double value) {
    throw new UnsupportedOperationException("Unsupport dynamic add for range view part now");
  }

  @Override
  public void add(int[] keys, double[] values) {
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
    return RowType.T_DOUBLE_DENSE;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    ByteBufSerdeUtils.serializeDoubles(output, values, startPos, endPos);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    values = ByteBufSerdeUtils.deserializeDoubles(input);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedDoublesLen(values, startPos, endPos);
  }
}
