package com.tencent.angel.psagent.matrix.transport.router.operator;

public interface IIntKeyLongValuePartOp {
  int[] getKeys();
  long[] getValues();
  void add(int key, long value);
  void add(int[] keys, long[] values);
}
