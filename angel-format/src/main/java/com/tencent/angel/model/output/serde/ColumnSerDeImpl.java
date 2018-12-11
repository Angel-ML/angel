package com.tencent.angel.model.output.serde;

import org.apache.hadoop.conf.Configuration;

public abstract class ColumnSerDeImpl implements ColumnSerDe {

  private final Configuration conf;

  public ColumnSerDeImpl(Configuration conf) {
    this.conf = conf;
  }
}
