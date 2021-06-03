package com.tencent.angel.psagent.matrix.transport.router.operator;

public interface IStringKeyIntValuePartOp {
  String[] getKeys();
  int[] getValues();
  void add(String key, int value);
  void add(String[] keys, int[] values);
}
