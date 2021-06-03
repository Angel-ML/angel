package com.tencent.angel.psagent.matrix.transport.router.hash;

import com.tencent.angel.common.collections.DynamicLongFloatArrayPair;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyFloatValuePartOp;
import io.netty.buffer.ByteBuf;

public class HashLongKeysFloatValuesPart extends HashKeyValuePart implements
    ILongKeyFloatValuePartOp {

  /**
   * Dynamic int array, it is only used in client(before serialization)
   */
  private DynamicLongFloatArrayPair dynamicKeysValues;

  /**
   * Final key array, it is only used in server(after de-serialization)
   */
  private long[] keys;

  /**
   * Final value array, it is only used in server(after de-serialization)
   */
  private float[] values;

  public HashLongKeysFloatValuesPart(int rowId, DynamicLongFloatArrayPair dynamicKeysValues) {
    super(rowId);
    this.dynamicKeysValues = dynamicKeysValues;
  }

  public HashLongKeysFloatValuesPart(int rowId, int size) {
    this(rowId, new DynamicLongFloatArrayPair(size));
  }

  public HashLongKeysFloatValuesPart(int size) {
    this(-1, new DynamicLongFloatArrayPair(size));
  }

  public HashLongKeysFloatValuesPart() {
    this(-1, null);
  }

  @Override
  public long[] getKeys() {
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
    return RowType.T_FLOAT_SPARSE_LONGKEY;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    dynamicKeysValues.serialize(output);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    dynamicKeysValues = new DynamicLongFloatArrayPair();
    dynamicKeysValues.deserialize(input);
    keys = dynamicKeysValues.getKeys();
    values = dynamicKeysValues.getValues();
  }

  @Override
  public int bufferLen() {
    return dynamicKeysValues.bufferLen();
  }

  @Override
  public void add(long key, float value) {
    dynamicKeysValues.add(key, value);
  }

  @Override
  public void add(long[] keys, float[] values) {
    for(int i = 0; i < keys.length; i++) {
      add(keys[i], values[i]);
    }
  }
}

