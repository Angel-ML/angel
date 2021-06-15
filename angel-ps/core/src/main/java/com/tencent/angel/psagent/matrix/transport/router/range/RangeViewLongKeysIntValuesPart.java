package com.tencent.angel.psagent.matrix.transport.router.range;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyIntValuePartOp;
import io.netty.buffer.ByteBuf;

public class RangeViewLongKeysIntValuesPart extends RangeKeyValuePart implements
    ILongKeyIntValuePartOp {
  /**
   * Whole key array before serialization, sorted by asc
   */
  private long[] keys;

  /**
   * Whole value array before serialization
   */
  private int[] values;

  public RangeViewLongKeysIntValuesPart(int rowId, long[] keys, int[] values, int startPos, int endPos) {
    super(rowId, startPos, endPos);
    this.keys = keys;
    this.values = values;
  }

  public RangeViewLongKeysIntValuesPart(long[] keys, int[] values, int startPos, int endPos) {
    this(-1, keys, values, startPos, endPos);
  }

  public RangeViewLongKeysIntValuesPart() {
    this(-1, null, null, -1, -1);
  }

  @Override
  public long[] getKeys() {
    return keys;
  }

  @Override
  public int[] getValues() {
    return values;
  }

  @Override
  public void add(long key, int value) {
    throw new UnsupportedOperationException("Unsupport dynamic add for range view part now");
  }

  @Override
  public void add(long[] keys, int[] values) {
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
    return RowType.T_INT_SPARSE_LONGKEY;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    ByteBufSerdeUtils.serializeInt(output, endPos - startPos);
    for(int i = startPos; i < endPos; i++) {
      ByteBufSerdeUtils.serializeLong(output, keys[i]);
      ByteBufSerdeUtils.serializeInt(output, values[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    int len = ByteBufSerdeUtils.deserializeInt(input);
    keys = new long[len];
    values = new int[len];
    for(int i = 0; i < len; i++) {
      keys[i] = ByteBufSerdeUtils.deserializeLong(input);
      values[i] = ByteBufSerdeUtils.deserializeInt(input);
    }
  }

  @Override
  public int bufferLen() {
    return super.bufferLen()
        + ByteBufSerdeUtils.INT_LENGTH
        + ByteBufSerdeUtils.LONG_LENGTH * (endPos - startPos)
        + ByteBufSerdeUtils.INT_LENGTH * (endPos - startPos);
  }
}
