package com.tencent.angel.psagent.matrix.transport.router.hash;

import com.tencent.angel.common.collections.DynamicLongDoubleArrayPair;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyDoubleValuePartOp;
import io.netty.buffer.ByteBuf;

public class HashLongKeysDoubleValuesPart extends HashKeyValuePart implements
    ILongKeyDoubleValuePartOp {

  /**
   * Dynamic int array, it is only used in client(before serialization)
   */
  private DynamicLongDoubleArrayPair dynamicKeysValues;

  /**
   * Final key array, it is only used in server(after de-serialization)
   */
  private long[] keys;

  /**
   * Final value array, it is only used in server(after de-serialization)
   */
  private double[] values;

  public HashLongKeysDoubleValuesPart(int rowId, DynamicLongDoubleArrayPair dynamicKeysValues) {
    super(rowId);
    this.dynamicKeysValues = dynamicKeysValues;
  }

  public HashLongKeysDoubleValuesPart(int rowId, int size) {
    this(rowId, new DynamicLongDoubleArrayPair(size));
  }

  public HashLongKeysDoubleValuesPart(int size) {
    this(-1, new DynamicLongDoubleArrayPair(size));
  }

  public HashLongKeysDoubleValuesPart() {
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
  public double[] getValues() {
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
    return RowType.T_DOUBLE_SPARSE_LONGKEY;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    dynamicKeysValues.serialize(output);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    dynamicKeysValues = new DynamicLongDoubleArrayPair();
    dynamicKeysValues.deserialize(input);
    keys = dynamicKeysValues.getKeys();
    values = dynamicKeysValues.getValues();
  }

  @Override
  public int bufferLen() {
    return dynamicKeysValues.bufferLen();
  }

  @Override
  public void add(long key, double value) {
    dynamicKeysValues.add(key, value);
  }

  @Override
  public void add(long[] keys, double[] values) {
    for(int i = 0; i < keys.length; i++) {
      add(keys[i], values[i]);
    }
  }
}
