package com.tencent.angel.psagent.matrix.transport.router.range;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.psagent.matrix.transport.router.operator.IIntKeyIntValuePartOp;
import io.netty.buffer.ByteBuf;

public class RangeViewIntKeysIntValuesPart extends RangeKeyValuePart implements
    IIntKeyIntValuePartOp {

  /**
   * Whole key array before serialization, sorted by asc
   */
  private int[] keys;

  /**
   * Whole value array before serialization
   */
  private int[] values;


  public RangeViewIntKeysIntValuesPart(int rowId, int[] keys, int[] values, int startPos, int endPos) {
    super(rowId, startPos, endPos);
    this.keys = keys;
    this.values = values;
  }

  public RangeViewIntKeysIntValuesPart(int[] keys, int[] values, int startPos, int endPos) {
    this(-1, keys, values, startPos, endPos);
  }

  public RangeViewIntKeysIntValuesPart() {
    this(-1, null, null, -1, -1);
  }

  @Override
  public int[] getKeys() {
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
    return RowType.T_INT_SPARSE;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    ByteBufSerdeUtils.serializeInt(output, endPos - startPos);
    for(int i = startPos; i < endPos; i++) {
      ByteBufSerdeUtils.serializeInt(output, keys[i]);
      ByteBufSerdeUtils.serializeInt(output, values[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    int len = ByteBufSerdeUtils.deserializeInt(input);
    keys = new int[len];
    values = new int[len];
    for(int i = 0; i < len; i++) {
      keys[i] = ByteBufSerdeUtils.deserializeInt(input);
      values[i] = ByteBufSerdeUtils.deserializeInt(input);
    }
  }

  @Override
  public int bufferLen() {
    return super.bufferLen()
        + ByteBufSerdeUtils.INT_LENGTH
        + ByteBufSerdeUtils.INT_LENGTH * (endPos - startPos)
        + ByteBufSerdeUtils.INT_LENGTH * (endPos - startPos);
  }
}
