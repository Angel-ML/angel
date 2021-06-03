package com.tencent.angel.common.collections;

import io.netty.buffer.ByteBuf;

public class DynamicStringDoubleArrayPair extends DynamicArray {

  private DynamicStringArray dynamicKeys;
  private DynamicDoubleArray dynamicValues;

  public DynamicStringDoubleArrayPair(int size) {
    dynamicKeys = new DynamicStringArray(size);
    dynamicValues = new DynamicDoubleArray(size);
  }

  public DynamicStringDoubleArrayPair() {
  }

  public void add(String key, double value) {
    dynamicKeys.add(key);
    dynamicValues.add(value);
  }

  public String[] getKeys() {
    return dynamicKeys.getData();
  }

  public double[] getValues() {
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
    dynamicValues = new DynamicDoubleArray();
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
