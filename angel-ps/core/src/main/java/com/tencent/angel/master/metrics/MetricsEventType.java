package com.tencent.angel.master.metrics;

/**
 * Event type for algorithm indexes service.
 */
public enum MetricsEventType {
  /** A task increment the iteration number */
  TASK_ITERATION_UPDATE,

  /** Set the algorithm indexes */
  ALGORITHM_METRICS_UPDATE
}
