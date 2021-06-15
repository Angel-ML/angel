package com.tencent.angel.psagent.matrix.transport.router.range;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.psagent.matrix.transport.router.operator.IIntKeyDoubleValuePartOp;
import io.netty.buffer.ByteBuf;

/**
 * Int keys and double values data partition view, it is only be used in Range partition
 */
public class RangeViewIntKeysDoubleValuesPart extends RangeKeyValuePart implements
    IIntKeyDoubleValuePartOp {

  /**
   * Whole key array before serialization, sorted by asc
   */
  private int[] keys;

  /**
   * Whole value array before serialization
   */
  private double[] values;


  public RangeViewIntKeysDoubleValuesPart(int rowId, int[] keys, double[] values, int startPos, int endPos) {
    super(rowId, startPos, endPos);
    this.keys = keys;
    this.values = values;
  }

  public RangeViewIntKeysDoubleValuesPart(int[] keys, double[] values, int startPos, int endPos) {
    this(-1, keys, values, startPos, endPos);
  }

  public RangeViewIntKeysDoubleValuesPart() {
    this(-1, null, null, -1, -1);
  }

  @Override
  public int[] getKeys() {
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
    return RowType.T_DOUBLE_SPARSE;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    ByteBufSerdeUtils.serializeInt(output, endPos - startPos);
    for(int i = startPos; i < endPos; i++) {
      ByteBufSerdeUtils.serializeInt(output, keys[i]);
      ByteBufSerdeUtils.serializeDouble(output, values[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    int len = ByteBufSerdeUtils.deserializeInt(input);
    keys = new int[len];
    values = new double[len];
    for(int i = 0; i < len; i++) {
      keys[i] = ByteBufSerdeUtils.deserializeInt(input);
      values[i] = ByteBufSerdeUtils.deserializeDouble(input);
    }
  }

  @Override
  public int bufferLen() {
    return super.bufferLen()
        + ByteBufSerdeUtils.INT_LENGTH
        + ByteBufSerdeUtils.INT_LENGTH * (endPos - startPos)
        + ByteBufSerdeUtils.DOUBLE_LENGTH * (endPos - startPos);
  }

  public void setKeys(int[] keys) {
    this.keys = keys;
  }

  public void setValues(double[] values) {
    this.values = values;
  }

  public int getStartPos() {
    return startPos;
  }

  public int getEndPos() {
    return endPos;
  }
}
