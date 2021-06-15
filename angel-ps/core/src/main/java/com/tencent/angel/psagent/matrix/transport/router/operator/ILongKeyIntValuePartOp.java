package com.tencent.angel.psagent.matrix.transport.router.operator;

public interface ILongKeyIntValuePartOp {
  long[] getKeys();
  int[] getValues();
  void add(long key, int value);
  void add(long[] keys, int[] values);
}
