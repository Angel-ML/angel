package com.tencent.angel.master.metrics;

import com.tencent.angel.ml.metrics.Metric;

import java.util.Map;

/**
 * Algorithm metrics update event
 */
public class MetricsUpdateEvent extends MetricsEvent {
  /** Metric name to Metric context map */
  private final Map<String, Metric> nameToMetrcMap;

  /**
   * Create a MetricsUpdateEvent
   * @param nameToMetrcMap Metric name to Metric context map
   */
  public MetricsUpdateEvent(Map<String, Metric> nameToMetrcMap){
    super(MetricsEventType.ALGORITHM_METRICS_UPDATE);
    this.nameToMetrcMap = nameToMetrcMap;
  }

  /**
   * Get Metric name to Metric context map
   * @return Metric name to Metric context map
   */
  public Map<String, Metric> getNameToMetrcMap(){
    return nameToMetrcMap;
  }
}
