package com.tencent.angel.common.collections;

import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;

public class DynamicStringObjectArrayPair extends DynamicArray {

  private DynamicStringArray dynamicKeys;
  private DynamicObjectArray dynamicValues;

  public DynamicStringObjectArrayPair(int size) {
    dynamicKeys = new DynamicStringArray(size);
    dynamicValues = new DynamicObjectArray(size);
  }

  public DynamicStringObjectArrayPair() {
  }

  public void add(String key, IElement value) {
    dynamicKeys.add(key);
    dynamicValues.add(value);
  }

  public String[] getKeys() {
    return dynamicKeys.getData();
  }

  public IElement[] getValues() {
    return dynamicValues.getData();
  }

  @Override
  public void serialize(ByteBuf out) {
    dynamicKeys.serialize(out);
    dynamicValues.serialize(out);
  }

  @Override
  public void deserialize(ByteBuf in) {
    dynamicKeys.deserialize(in);
    dynamicValues.deserialize(in);
  }

  @Override
  public int bufferLen() {
    return dynamicKeys.bufferLen() + dynamicValues.bufferLen();
  }

  @Override
  public int size() {
    if(dynamicKeys != null) {
      return dynamicKeys.size();
    } else {
      return 0;
    }
  }
}
