package com.tencent.angel.model.output.serde;

import org.apache.hadoop.conf.Configuration;

public abstract class ElementSerDeImpl implements ElementSerDe {

  private final Configuration conf;

  public ElementSerDeImpl(Configuration conf) {
    this.conf = conf;
  }
}
