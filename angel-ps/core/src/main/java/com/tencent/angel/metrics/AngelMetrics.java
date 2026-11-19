package com.tencent.angel.metrics;

import java.util.concurrent.atomic.AtomicLong;

public abstract class AngelMetrics implements IAngelMetrics {

  protected final AtomicLong counter = new AtomicLong(0);
  public long getCounter() {
    return counter.get();
  }
}
