package com.tencent.angel.psagent.matrix.transport.router.operator;

public interface ILongKeyDoubleValuePartOp {
  long[] getKeys();
  double[] getValues();
  void add(long key, double value);
  void add(long[] keys, double[] values);
}
