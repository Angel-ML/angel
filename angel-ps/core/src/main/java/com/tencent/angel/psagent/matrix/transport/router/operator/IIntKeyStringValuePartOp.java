package com.tencent.angel.psagent.matrix.transport.router.operator;

public interface IIntKeyStringValuePartOp {
  int[] getKeys();
  String[] getValues();
  void add(int key, String value);
  void add(int[] keys, String[] values);
}
