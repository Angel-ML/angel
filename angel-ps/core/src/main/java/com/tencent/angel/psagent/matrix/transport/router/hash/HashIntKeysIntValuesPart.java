package com.tencent.angel.psagent.matrix.transport.router.hash;

import com.tencent.angel.common.collections.DynamicIntIntArrayPair;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.psagent.matrix.transport.router.operator.IIntKeyIntValuePartOp;
import io.netty.buffer.ByteBuf;

public class HashIntKeysIntValuesPart extends HashKeyValuePart implements IIntKeyIntValuePartOp {


  private DynamicIntIntArrayPair dynamicKeysValues;

  /**
   * Final key array, it is only used in server(after de-serialization)
   */
  private int[] keys;

  /**
   * Final value array, it is only used in server(after de-serialization)
   */
  private int[] values;

  public HashIntKeysIntValuesPart(int rowId, DynamicIntIntArrayPair dynamicKeysValues) {
    super(rowId);
    this.dynamicKeysValues = dynamicKeysValues;
  }

  public HashIntKeysIntValuesPart(int rowId, int size) {
    this(rowId, new DynamicIntIntArrayPair(size));
  }

  public HashIntKeysIntValuesPart(int size) {
    this(-1, new DynamicIntIntArrayPair(size));
  }

  public HashIntKeysIntValuesPart() {
    this(-1, null);
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
    return RowType.T_INT_SPARSE;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    dynamicKeysValues.serialize(output);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    dynamicKeysValues = new DynamicIntIntArrayPair();
    dynamicKeysValues.deserialize(input);
    keys = dynamicKeysValues.getKeys();
    values = dynamicKeysValues.getValues();
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + dynamicKeysValues.bufferLen();
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
  public int[] getValues() {
    // It is recommended not to call this method on the client
    if(values == null) {
      values = dynamicKeysValues.getValues();
    }
    return values;
  }

  @Override
  public void add(int key, int value) {
    dynamicKeysValues.add(key, value);
  }

  @Override
  public void add(int[] keys, int[] values) {
    for(int i = 0; i < keys.length; i++) {
      add(keys[i], values[i]);
    }
  }
}
