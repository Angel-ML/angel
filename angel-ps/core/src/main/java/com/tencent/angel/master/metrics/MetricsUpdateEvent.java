/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.master.metrics;

import com.tencent.angel.ml.metric.Metric;

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
