package com.tencent.angel.psagent.matrix.transport.router.hash;

import com.tencent.angel.common.collections.DynamicStringObjectArrayPair;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.operator.IStringKeyAnyValuePartOp;
import io.netty.buffer.ByteBuf;

public class HashStringKeysAnyValuesPart extends HashKeyValuePart implements
    IStringKeyAnyValuePartOp {
  /**
   * Dynamic int array, it is only used in client(before serialization)
   */
  private DynamicStringObjectArrayPair dynamicKeysValues;

  /**
   * Final key array, it is only used in server(after de-serialization)
   */
  private String[] keys;

  /**
   * Final value array, it is only used in server(after de-serialization)
   */
  private IElement[] values;

  public HashStringKeysAnyValuesPart(int rowId, DynamicStringObjectArrayPair dynamicKeysValues) {
    super(rowId);
    this.dynamicKeysValues = dynamicKeysValues;
  }

  public HashStringKeysAnyValuesPart(int rowId, int size) {
    this(rowId, new DynamicStringObjectArrayPair(size));
  }

  public HashStringKeysAnyValuesPart(int size) {
    this(-1, new DynamicStringObjectArrayPair(size));
  }

  public HashStringKeysAnyValuesPart() {
    this(-1, null);
  }


  @Override
  public RowType getKeyValueType() {
    return RowType.T_ANY_STRINGKEY_SPARSE;
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
  public String[] getKeys() {
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
    dynamicKeysValues = new DynamicStringObjectArrayPair();
    dynamicKeysValues.deserialize(input);
    keys = dynamicKeysValues.getKeys();
    values = dynamicKeysValues.getValues();
  }

  @Override
  public int bufferLen() {
    return dynamicKeysValues.bufferLen();
  }


  @Override
  public void add(String key, IElement value) {
    dynamicKeysValues.add(key, value);
  }

  @Override
  public void add(String[] keys, IElement[] values) {
    for(int i = 0; i < keys.length; i++) {
      add(keys[i], values[i]);
    }
  }
}
