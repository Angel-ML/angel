package com.tencent.angel.psagent.matrix.transport.router.hash;

import com.tencent.angel.common.collections.DynamicIntFloatArrayPair;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.psagent.matrix.transport.router.operator.IIntKeyFloatValuePartOp;
import io.netty.buffer.ByteBuf;

public class HashIntKeysFloatValuesPart extends HashKeyValuePart implements
    IIntKeyFloatValuePartOp {

  private DynamicIntFloatArrayPair dynamicKeysValues;

  /**
   * Final key array, it is only used in server(after de-serialization)
   */
  private int[] keys;

  /**
   * Final value array, it is only used in server(after de-serialization)
   */
  private float[] values;

  public HashIntKeysFloatValuesPart(int rowId, DynamicIntFloatArrayPair dynamicKeysValues) {
    super(rowId);
    this.dynamicKeysValues = dynamicKeysValues;
  }

  public HashIntKeysFloatValuesPart(int rowId, int size) {
    this(rowId, new DynamicIntFloatArrayPair(size));
  }

  public HashIntKeysFloatValuesPart(int size) {
    this(-1, new DynamicIntFloatArrayPair(size));
  }

  public HashIntKeysFloatValuesPart() {
    this(-1, null);
  }

  @Override
  public int[] getKeys() {
    // It is recommended not to call this method on the client
    if(keys == null) {
      keys = dynamicKeysValues.getKeys();
    }
    return keys;
  }

  @Override
  public float[] getValues() {
    // It is recommended not to call this method on the client
    if(values == null) {
      values = dynamicKeysValues.getValues();
    }
    return values;
  }

  @Override
  public int size() {
    if(keys != null) {
      return keys.length;
    } else {
      return dynamicKeysValues.size();
    }
  }

  @Override
  public RowType getKeyValueType() {
    return RowType.T_FLOAT_SPARSE;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    dynamicKeysValues.serialize(output);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    dynamicKeysValues = new DynamicIntFloatArrayPair();
    dynamicKeysValues.deserialize(input);
    keys = dynamicKeysValues.getKeys();
    values = dynamicKeysValues.getValues();
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + dynamicKeysValues.bufferLen();
  }

  public void add(int key, float value) {
    dynamicKeysValues.add(key, value);
  }

  @Override
  public void add(int[] keys, float[] values) {
    for(int i = 0; i < keys.length; i++) {
      add(keys[i], values[i]);
    }
  }
}
