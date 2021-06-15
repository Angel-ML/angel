package com.tencent.angel.psagent.matrix.transport.router.operator;

public interface IIntKeyDoubleValuePartOp {
  int[] getKeys();
  double[] getValues();
  void add(int key, double value);
  void add(int[] keys, double[] values);
}
