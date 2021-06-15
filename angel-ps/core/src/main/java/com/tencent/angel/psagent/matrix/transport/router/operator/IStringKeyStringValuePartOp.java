package com.tencent.angel.psagent.matrix.transport.router.operator;

public interface IStringKeyStringValuePartOp {
  String[] getKeys();
  String[] getValues();
  void add(String key, String value);
  void add(String[] keys, String[] values);
}
