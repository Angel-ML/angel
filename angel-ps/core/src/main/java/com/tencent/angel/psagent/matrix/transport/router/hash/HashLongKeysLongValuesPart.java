package com.tencent.angel.psagent.matrix.transport.router.hash;

import com.tencent.angel.common.collections.DynamicLongLongArrayPair;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyLongValuePartOp;
import io.netty.buffer.ByteBuf;

public class HashLongKeysLongValuesPart extends HashKeyValuePart implements
    ILongKeyLongValuePartOp {

  /**
   * Dynamic int array, it is only used in client(before serialization)
   */
  private DynamicLongLongArrayPair dynamicKeysValues;

  /**
   * Final key array, it is only used in server(after de-serialization)
   */
  private long[] keys;

  /**
   * Final value array, it is only used in server(after de-serialization)
   */
  private long[] values;

  public HashLongKeysLongValuesPart(int rowId, DynamicLongLongArrayPair dynamicKeysValues) {
    super(rowId);
    this.dynamicKeysValues = dynamicKeysValues;
  }

  public HashLongKeysLongValuesPart(int rowId, int size) {
    this(rowId, new DynamicLongLongArrayPair(size));
  }

  public HashLongKeysLongValuesPart(int size) {
    this(-1, new DynamicLongLongArrayPair(size));
  }

  public HashLongKeysLongValuesPart() {
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
  public long[] getValues() {
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
    return RowType.T_LONG_SPARSE_LONGKEY;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    dynamicKeysValues.serialize(output);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    dynamicKeysValues = new DynamicLongLongArrayPair();
    dynamicKeysValues.deserialize(input);
    keys = dynamicKeysValues.getKeys();
    values = dynamicKeysValues.getValues();
  }

  @Override
  public int bufferLen() {
    return dynamicKeysValues.bufferLen();
  }

  @Override
  public void add(long key, long value) {
    dynamicKeysValues.add(key, value);
  }

  @Override
  public void add(long[] keys, long[] values) {
    for(int i = 0; i < keys.length; i++) {
      add(keys[i], values[i]);
    }
  }
}
