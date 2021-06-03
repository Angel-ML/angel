package com.tencent.angel.psagent.matrix.transport.router.hash;

import com.tencent.angel.common.collections.DynamicObjectObjectArrayPair;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.operator.IAnyKeyAnyValuePartOp;
import io.netty.buffer.ByteBuf;

public class HashAnyKeysAnyValuesPart extends HashKeyValuePart implements IAnyKeyAnyValuePartOp {
  /**
   * Dynamic int array, it is only used in client(before serialization)
   */
  private DynamicObjectObjectArrayPair dynamicKeysValues;

  /**
   * Final key array, it is only used in server(after de-serialization)
   */
  private IElement[] keys;

  /**
   * Final value array, it is only used in server(after de-serialization)
   */
  private IElement[] values;

  public HashAnyKeysAnyValuesPart(int rowId, DynamicObjectObjectArrayPair dynamicKeysValues) {
    super(rowId);
    this.dynamicKeysValues = dynamicKeysValues;
  }

  public HashAnyKeysAnyValuesPart(int rowId, int size) {
    this(rowId, new DynamicObjectObjectArrayPair(size));
  }

  public HashAnyKeysAnyValuesPart(int size) {
    this(-1, new DynamicObjectObjectArrayPair(size));
  }

  public HashAnyKeysAnyValuesPart() {
    this(-1, null);
  }


  @Override
  public RowType getKeyValueType() {
    return RowType.T_ANY_ANYKEY_SPARSE;
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
  public IElement[] getKeys() {
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
    dynamicKeysValues = new DynamicObjectObjectArrayPair();
    dynamicKeysValues.deserialize(input);
    keys = dynamicKeysValues.getKeys();
    values = dynamicKeysValues.getValues();
  }

  @Override
  public int bufferLen() {
    return dynamicKeysValues.bufferLen();
  }


  @Override
  public void add(IElement key, IElement value) {
    dynamicKeysValues.add(key, value);
  }

  @Override
  public void add(IElement[] keys, IElement[] values) {
    for(int i = 0; i < keys.length; i++) {
      add(keys[i], values[i]);
    }
  }
}
