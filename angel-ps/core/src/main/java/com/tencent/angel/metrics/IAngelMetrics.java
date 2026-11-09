package com.tencent.angel.metrics;

public interface IAngelMetrics {
  String getAppId();
  void merge(MetricsItem metrics);
  String generateTextMessage(String sep);
}
