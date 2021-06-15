package com.tencent.angel.psagent.matrix.transport.router.operator;

public interface IStringKeyLongValuePartOp {
  String[] getKeys();
  long[] getValues();
  void add(String key, long value);
  void add(String[] keys, long[] values);
}
