package com.tencent.angel.psagent.matrix.transport.router.hash;

import com.tencent.angel.common.collections.DynamicIntObjectArrayPair;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.operator.IIntKeyAnyValuePartOp;
import io.netty.buffer.ByteBuf;

public class HashIntKeysAnyValuesPart extends HashKeyValuePart implements IIntKeyAnyValuePartOp {
  /**
   * Dynamic int array, it is only used in client(before serialization)
   */
  private DynamicIntObjectArrayPair dynamicKeysValues;

  /**
   * Final key array, it is only used in server(after de-serialization)
   */
  private int[] keys;

  /**
   * Final value array, it is only used in server(after de-serialization)
   */
  private IElement[] values;

  public HashIntKeysAnyValuesPart(int rowId, DynamicIntObjectArrayPair dynamicKeysValues) {
    super(rowId);
    this.dynamicKeysValues = dynamicKeysValues;
  }

  public HashIntKeysAnyValuesPart(int rowId, int size) {
    this(rowId, new DynamicIntObjectArrayPair(size));
  }

  public HashIntKeysAnyValuesPart(int size) {
    this(-1, new DynamicIntObjectArrayPair(size));
  }

  public HashIntKeysAnyValuesPart() {
    this(-1, null);
  }


  @Override
  public RowType getKeyValueType() {
    return RowType.T_ANY_INTKEY_SPARSE;
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
  public int[] getKeys() {
    // It is recommended not to call this method on the client
    if(keys == null) {
      keys = dynamicKeysValues.getKeys();
    }
    return keys;
  }

  @Override
  public IElement[] getValues() {
    // It is recommended not to call this method on the client
    if(values == null) {
      values = dynamicKeysValues.getValues();
    }
    return values;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    dynamicKeysValues.serialize(output);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    dynamicKeysValues = new DynamicIntObjectArrayPair();
    dynamicKeysValues.deserialize(input);
    keys = dynamicKeysValues.getKeys();
    values = dynamicKeysValues.getValues();
  }

  @Override
  public int bufferLen() {
    return dynamicKeysValues.bufferLen();
  }


  @Override
  public void add(int key, IElement value) {
    dynamicKeysValues.add(key, value);
  }

  @Override
  public void add(int[] keys, IElement[] values) {
    for(int i = 0; i < keys.length; i++) {
      add(keys[i], values[i]);
    }
  }
}
