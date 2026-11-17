package com.tencent.angel.metrics;

/**
 * collect angel ps metrics
 */
public class AngelPSMetrics extends AngelMetrics {

  private final long timestamp;//timestamp parsed by applicationId
  private final String appId;//ps master applicationId
  private final String jvmTaskId;//ps containerId
  private final long startTime;
  private long endTime;

  public AngelPSMetrics(String appId, String jvmTaskId) {
    this.timestamp = System.currentTimeMillis();
    this.appId = appId;
    this.jvmTaskId = jvmTaskId;
    this.startTime = System.currentTimeMillis();
    this.endTime = System.currentTimeMillis();
  }

  @Override
  public String getAppId() {
    return this.appId;
  }

  @Override
  public void merge(MetricsItem metrics) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String generateTextMessage(String sep) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(timestamp);
    stringBuilder.append(sep);
    stringBuilder.append(appId);
    stringBuilder.append(sep);
    stringBuilder.append(jvmTaskId);
    stringBuilder.append(sep);
    stringBuilder.append(startTime);
    stringBuilder.append(sep);
    stringBuilder.append(endTime);
    return stringBuilder.toString();
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }
}
