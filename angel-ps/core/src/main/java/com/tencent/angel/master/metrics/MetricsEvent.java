package com.tencent.angel.master.metrics;

import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * Event for algorithm indexes service.
 */
public class MetricsEvent extends AbstractEvent<MetricsEventType> {
  public MetricsEvent(MetricsEventType algoMetricsEventType) {
    super(algoMetricsEventType);
  }
}
