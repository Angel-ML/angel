package com.tencent.angel.psagent.matrix.transport.router.operator;

public interface ILongKeyFloatValuePartOp {
  long[] getKeys();
  float[] getValues();
  void add(long key, float value);
  void add(long[] keys, float[] values);
}
