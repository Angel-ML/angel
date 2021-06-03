package com.tencent.angel.psagent.matrix.transport.router.operator;

public interface IIntKeyFloatValuePartOp {
  int[] getKeys();
  float[] getValues();
  void add(int key, float value);
  void add(int[] keys, float[] values);
}
