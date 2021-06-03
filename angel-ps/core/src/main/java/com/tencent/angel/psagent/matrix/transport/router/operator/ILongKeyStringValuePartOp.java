package com.tencent.angel.psagent.matrix.transport.router.operator;

public interface ILongKeyStringValuePartOp {
  long[] getKeys();
  String[] getValues();
  void add(long key, String value);
  void add(long[] keys, String[] values);
}
