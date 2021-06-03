package com.tencent.angel.psagent.matrix.transport.router.operator;

public interface IStringKeyFloatValuePartOp {
  String[] getKeys();
  float[] getValues();
  void add(String key, float value);
  void add(String[] keys, float[] values);
}
