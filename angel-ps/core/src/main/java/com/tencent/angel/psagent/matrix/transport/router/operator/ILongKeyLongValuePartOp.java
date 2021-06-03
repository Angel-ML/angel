package com.tencent.angel.psagent.matrix.transport.router.operator;

public interface ILongKeyLongValuePartOp {
  long[] getKeys();
  long[] getValues();
  void add(long key, long value);
  void add(long[] keys, long[] values);
}
