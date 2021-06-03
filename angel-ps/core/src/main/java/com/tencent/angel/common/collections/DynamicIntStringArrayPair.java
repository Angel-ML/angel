package com.tencent.angel.common.collections;

import io.netty.buffer.ByteBuf;

public class DynamicIntStringArrayPair extends DynamicArray {


  private DynamicIntArray dynamicKeys;
  private DynamicStringArray dynamicValues;

  public DynamicIntStringArrayPair(int size) {
    dynamicKeys = new DynamicIntArray(size);
    dynamicValues = new DynamicStringArray(size);
  }

  public DynamicIntStringArrayPair() {

  }

  public void add(int key, String value) {
    dynamicKeys.add(key);
    dynamicValues.add(value);
  }

  public int[] getKeys() {
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
    dynamicKeys = new DynamicIntArray();
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
