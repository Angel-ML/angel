package com.tencent.angel.common.collections;

import io.netty.buffer.ByteBuf;

public class DynamicStringLongArrayPair extends DynamicArray {

  private DynamicStringArray dynamicKeys;
  private DynamicLongArray dynamicValues;

  public DynamicStringLongArrayPair(int size) {
    dynamicKeys = new DynamicStringArray(size);
    dynamicValues = new DynamicLongArray(size);
  }

  public DynamicStringLongArrayPair() {
  }

  public void add(String key, long value) {
    dynamicKeys.add(key);
    dynamicValues.add(value);
  }

  public String[] getKeys() {
    return dynamicKeys.getData();
  }

  public long[] getValues() {
    return dynamicValues.getData();
  }

  @Override
  public void serialize(ByteBuf out) {
    dynamicKeys.serialize(out);
    dynamicValues.serialize(out);
  }

  @Override
  public void deserialize(ByteBuf in) {
    dynamicKeys = new DynamicStringArray();
    dynamicKeys.deserialize(in);
    dynamicValues = new DynamicLongArray();
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
