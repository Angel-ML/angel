package com.tencent.angel.common.collections;

import io.netty.buffer.ByteBuf;

public class DynamicLongStringArrayPair extends DynamicArray {

  private DynamicLongArray dynamicKeys;
  private DynamicStringArray dynamicValues;

  public DynamicLongStringArrayPair(int size) {
    dynamicKeys = new DynamicLongArray(size);
    dynamicValues = new DynamicStringArray(size);
  }

  public DynamicLongStringArrayPair() {
  }

  public void add(long key, String value) {
    dynamicKeys.add(key);
    dynamicValues.add(value);
  }

  public long[] getKeys() {
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
    dynamicKeys = new DynamicLongArray();
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
