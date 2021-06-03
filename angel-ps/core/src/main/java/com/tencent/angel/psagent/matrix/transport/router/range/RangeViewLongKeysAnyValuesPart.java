package com.tencent.angel.psagent.matrix.transport.router.range;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyAnyValuePartOp;
import io.netty.buffer.ByteBuf;

public class RangeViewLongKeysAnyValuesPart extends RangeKeyValuePart implements
    ILongKeyAnyValuePartOp {
  /**
   * Whole key array before serialization, sorted by asc
   */
  private long[] keys;

  /**
   * Whole value array before serialization
   */
  private IElement[] values;

  public RangeViewLongKeysAnyValuesPart(int rowId, long[] keys, IElement[] values, int startPos, int endPos) {
    super(rowId, startPos, endPos);
    this.keys = keys;
    this.values = values;
  }

  public RangeViewLongKeysAnyValuesPart(long[] keys, IElement[] values, int startPos, int endPos) {
    this(-1, keys, values, startPos, endPos);
  }

  public RangeViewLongKeysAnyValuesPart() {
    this(-1, null, null, -1, -1);
  }

  @Override
  public long[] getKeys() {
    return keys;
  }

  @Override
  public IElement[] getValues() {
    return values;
  }

  @Override
  public void add(long key, IElement value) {
    throw new UnsupportedOperationException("Unsupport dynamic add for range view part now");
  }

  @Override
  public void add(long[] keys, IElement[] values) {
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
    return RowType.T_ANY_LONGKEY_SPARSE;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    ByteBufSerdeUtils.serializeInt(output, endPos - startPos);
    if(endPos - startPos > 0) {
      // IElement class name
      ByteBufSerdeUtils.serializeUTF8(output, values[startPos].getClass().getName());
      for(int i = startPos; i < endPos; i++) {
        ByteBufSerdeUtils.serializeLong(output, keys[i]);
        values[i].serialize(output);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    int len = ByteBufSerdeUtils.deserializeInt(input);
    if(len > 0) {
      keys = new long[len];
      values = new IElement[len];
      try {
        String valueClassName = ByteBufSerdeUtils.deserializeUTF8(input);
        Class valueClass = Class.forName(valueClassName);
        for(int i = 0; i < len; i++) {
          keys[i] = ByteBufSerdeUtils.deserializeLong(input);
          values[i] = (IElement) valueClass.newInstance();
          values[i].deserialize(input);
        }
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += ByteBufSerdeUtils.INT_LENGTH;
    if(endPos - startPos > 0) {
      // IElement class name
      len += ByteBufSerdeUtils.serializedUTF8Len(values[startPos].getClass().getName());
      for(int i = startPos; i < endPos; i++) {
        len += ByteBufSerdeUtils.LONG_LENGTH;
        len += values[i].bufferLen();
      }
    }
    return len;
  }
}
