package com.tencent.angel.common.collections;

import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;

public class DynamicObjectStringArrayPair extends DynamicArray {

  private DynamicObjectArray dynamicKeys;
  private DynamicStringArray dynamicValues;

  public DynamicObjectStringArrayPair(int size) {
    dynamicKeys = new DynamicObjectArray(size);
    dynamicValues = new DynamicStringArray(size);
  }

  public DynamicObjectStringArrayPair() {
  }

  public void add(IElement key, String value) {
    dynamicKeys.add(key);
    dynamicValues.add(value);
  }

  public IElement[] getKeys() {
    return dynamicKeys.getData();
  }

  public String[] getValues() {
    return dynamicValues.getData();
  }

  @Override
  public void serialize(ByteBuf out) {
    dynamicKeys.serialize(out);
    dynamicValues.serialize(out);
  }

  @Override
  public void deserialize(ByteBuf in) {
    dynamicKeys = new DynamicObjectArray();
    dynamicKeys.deserialize(in);
    dynamicValues = new DynamicStringArray();
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
