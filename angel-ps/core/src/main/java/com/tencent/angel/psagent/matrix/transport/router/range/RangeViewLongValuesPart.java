package com.tencent.angel.psagent.matrix.transport.router.range;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.psagent.matrix.transport.router.operator.IIntKeyLongValuePartOp;
import io.netty.buffer.ByteBuf;

public class RangeViewLongValuesPart extends RangeKeyValuePart implements IIntKeyLongValuePartOp {
  /**
   * Whole key array before serialization, sorted by asc
   */
  private int[] keys;

  /**
   * Whole value array before serialization
   */
  private long[] values;


  public RangeViewLongValuesPart(int rowId, long[] values, int startPos, int endPos) {
    super(rowId, startPos, endPos);
    this.values = values;
  }

  public RangeViewLongValuesPart(long[] values, int startPos, int endPos) {
    this(-1, values, startPos, endPos);
  }

  public RangeViewLongValuesPart() {
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
  public long[] getValues() {
    return values;
  }

  @Override
  public void add(int key, long value) {
    throw new UnsupportedOperationException("Unsupport dynamic add for range view part now");
  }

  @Override
  public void add(int[] keys, long[] values) {
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
    return RowType.T_LONG_DENSE;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    ByteBufSerdeUtils.serializeLongs(output, values, startPos, endPos);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    values = ByteBufSerdeUtils.deserializeLongs(input);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedLongsLen(values, startPos, endPos);
  }
}
