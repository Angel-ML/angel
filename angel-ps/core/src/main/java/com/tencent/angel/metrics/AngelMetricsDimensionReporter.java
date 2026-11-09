package com.tencent.angel.metrics;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;

/**
 * Base class for reporters that push dimension-tagged metrics (e.g. per-PS resource usage) to an
 * external monitoring backend. Implementations are plugged in via
 * {@link com.tencent.angel.conf.AngelConf#ANGEL_METRICS_ADS_REPORTER}.
 */
public abstract class AngelMetricsDimensionReporter implements AngelMetricsReporter {

  public abstract void report(Map<String, String> dimensions, Map<String, String> simpleMetrics, String method);

  public abstract void report(Map<String, String> dimensions);

  @Override
  public void send(String message, String tid) {

  }

  @Override
  public void send(List<String> message, String tid) {

  }

  @Override
  public void send(String message, Configuration conf, String tid) {

  }

  @Override
  public void send(List<String> message, Configuration conf, String tid) {

  }

  @Override
  public void asyncSend(String message, String tid) {

  }

  @Override
  public void asyncSend(List<String> message, String tid) {

  }

  @Override
  public void asyncSend(String message, Configuration conf, String tid) {

  }

  @Override
  public void asyncSend(List<String> message, Configuration conf, String tid) {

  }

  @Override
  public void close() {

  }
}
