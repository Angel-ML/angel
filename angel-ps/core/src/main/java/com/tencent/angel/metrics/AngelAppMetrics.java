package com.tencent.angel.metrics;

/**
 * collect angel app metrics
 */
public class AngelAppMetrics extends AngelMetrics {

  private long timestamp;//timestamp parsed by applicationId
  private String appId; //driver applicationId
  private String settings; //settings about angel job: such as: resources, ip, port etc.
  private String psAppTd = ""; //ps applicationId
  private String appName; //app name
  private long psInstances; //number of ps
  private long psCores; //number of ps cores
  private long psMemory; //memory of ps(byte)
  private long totalMemory; // total memory of input data
  private String dataMemory; //memory of input data, save as json string
  private long waitPSStartCost = -1; //time-consuming for waiting start-up of ps
  private long startTime;
  private long endTime;
  private String failureReason = "";
  private long dataPartitionNum;
  private long psPartitionNum;

  public AngelAppMetrics(String appId, String settings, String appName, long psInstances,
      long psCores, long psMemory, long totalMemory, String dataMemory, long startTime,
      long dataPartitionNum, long psPartitionNum) {
    this.timestamp = System.currentTimeMillis();
    this.appId = appId;
    this.settings = settings;
    this.appName = appName;
    this.psInstances = psInstances;
    this.psCores = psCores;
    this.psMemory = psMemory;
    this.totalMemory = totalMemory;
    this.dataMemory = dataMemory;
    this.startTime = startTime;
    this.endTime = startTime;
    this.dataPartitionNum = dataPartitionNum;
    this.psPartitionNum = psPartitionNum;
  }

  public void setPsAppTd(String psAppTd) {
    this.psAppTd = psAppTd;
  }

  public void setWaitPSStartCost(long waitPSStartCost) {
    this.waitPSStartCost = waitPSStartCost;
  }

  public void setEndTime(long endTime) {
    this.endTime = endTime;
  }

  public void setFailureReason(String failureReason) {
    this.failureReason = failureReason;
  }

  @Override
  public String getAppId() {
    return appId;
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
    stringBuilder.append(settings);
    stringBuilder.append(sep);
    stringBuilder.append(psAppTd);
    stringBuilder.append(sep);
    stringBuilder.append(appName);
    stringBuilder.append(sep);
    stringBuilder.append(psInstances);
    stringBuilder.append(sep);
    stringBuilder.append(psCores);
    stringBuilder.append(sep);
    stringBuilder.append(psMemory);
    stringBuilder.append(sep);
    stringBuilder.append(totalMemory);
    stringBuilder.append(sep);
    stringBuilder.append(dataMemory);
    stringBuilder.append(sep);
    stringBuilder.append(waitPSStartCost);
    stringBuilder.append(sep);
    stringBuilder.append(startTime);
    stringBuilder.append(sep);
    stringBuilder.append(endTime);
    stringBuilder.append(sep);
    stringBuilder.append(failureReason);
    stringBuilder.append(sep);
    stringBuilder.append(dataPartitionNum);
    stringBuilder.append(sep);
    stringBuilder.append(psPartitionNum);
    return stringBuilder.toString();
  }
}
