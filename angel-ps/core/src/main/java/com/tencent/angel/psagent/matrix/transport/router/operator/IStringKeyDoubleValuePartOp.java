package com.tencent.angel.psagent.matrix.transport.router.operator;

public interface IStringKeyDoubleValuePartOp {
  String[] getKeys();
  double[] getValues();
  void add(String key, double value);
  void add(String[] keys, double[] values);
}
