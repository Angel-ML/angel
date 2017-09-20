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

import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.ml.metrics.Metric;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.event.EventHandler;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Algorithm log service. It summary all task counters to generate global counters, then calculate
 * the algorithm indexes use these global counters. It holds the algorithm indexes in memory for
 * angel client and write them to hdfs also.
 */
public class MetricsService extends AbstractService implements EventHandler<MetricsEvent> {
  static final Log LOG = LogFactory.getLog(MetricsService.class);
  /** Application context */
  private final AMContext context;

  /** Iteration number -> (algorithm metric name -> value)*/
  private final Map<Integer, Map<String, Double>> iterToMetricsMap;

  /** Algorithm metric name to Metric map */
  private final Map<String, Metric> metricsCache;

  /** Algorithm index calculate thread */
  private Thread handler;

  /** Event queue */
  private final LinkedBlockingDeque<MetricsEvent> eventQueue;

  /** Stopped the service */
  private final AtomicBoolean stopped;

  /** Current iteration number */
  private volatile int currentIter;

  /** Log file writter */
  private volatile DistributeLog logWritter;

  private volatile boolean needWriteName;

  /**
   * Construct the service.
   */
  public MetricsService(AMContext context) {
    super("algorithm-metrics-service");
    this.context = context;
    iterToMetricsMap = new ConcurrentHashMap<>();
    metricsCache = new LinkedHashMap<>();
    eventQueue = new LinkedBlockingDeque<>();
    stopped = new AtomicBoolean(false);
    currentIter = 0;
  }

  /**
   * Get current iteration number
   * @return int current iteration number
   */
  public int getCurrentIter() {
    return currentIter;
  }

  /**
   * Get algorithm indexes
   * @param itertionNum iteration number
   * @return Map<String, Double> algorithm name to value map
   */
  public Map<String, Double> getAlgoMetrics(int itertionNum) {
    return iterToMetricsMap.get(itertionNum);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    logWritter = new DistributeLog(conf);
    needWriteName = true;
    try {
      logWritter.init();
    } catch (Exception x) {
      LOG.error("init log writter failed ", x);
      logWritter = null;
    }
  }

  @Override
  protected void serviceStart() throws Exception {
    handler = new Thread() {
      @SuppressWarnings("unchecked")
      @Override
      public void run() {
        MetricsEvent event = null;
        while (!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            event = eventQueue.take();
            switch (event.getType()) {
              case ALGORITHM_METRICS_UPDATE:
                mergeAlgoMetrics(((MetricsUpdateEvent) event).getNameToMetrcMap());
                break;

              case TASK_ITERATION_UPDATE: {
                int minIter = context.getWorkerManager().getMinIteration();
                if(minIter > currentIter) {
                  calAlgoMetrics(minIter);
                  currentIter = minIter;
                }
                break;
              }

              default:
                break;
            }
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.error("algorithm log event handler is interrupted. " + e);
            }
            return;
          }
        }
      }
    };
    handler.setName("algo-log-event-handler");
    handler.start();
  }

  @Override
  protected void serviceStop(){
    if(!stopped.getAndSet(true)) {
      if(handler != null) {
        handler.interrupt();
        try {
          handler.join(1000);
        } catch (InterruptedException e) {

        }

        handler = null;
      }

      if(logWritter != null) {
        try {
          logWritter.close();
        } catch (IOException e) {

        }
        logWritter = null;
      }
    }
  }

  private void mergeAlgoMetrics(Map<String, Metric> nameToMetricMap) {
    for(Map.Entry<String, Metric> metricEntry:nameToMetricMap.entrySet()) {
      Metric oldMetric = metricsCache.get(metricEntry.getKey());
      if(oldMetric == null) {
        metricsCache.put(metricEntry.getKey(), metricEntry.getValue());
      } else {
        oldMetric.merge(metricEntry.getValue());
      }
    }
  }

  private void calAlgoMetrics(int iter) {
    LinkedHashMap<String, Double> nameToMetricMap = new LinkedHashMap<>(metricsCache.size());
    for(Map.Entry<String, Metric> metricEntry:metricsCache.entrySet()) {
      nameToMetricMap.put(metricEntry.getKey(), metricEntry.getValue().calculate());
    }
    iterToMetricsMap.put(iter, nameToMetricMap);
    metricsCache.clear();

    if(logWritter != null) {
      try {
        if(needWriteName) {
          List<String> names = new ArrayList<> (nameToMetricMap.size());
          for(Map.Entry<String, Double> metricEntry:nameToMetricMap.entrySet()) {
            names.add(metricEntry.getKey());
          }
          logWritter.setNames(names);
          needWriteName = false;
        }
        logWritter.writeLog(nameToMetricMap);
      } catch (IOException e) {
        LOG.error("write index values to file failed ", e);
      }
    }

    LOG.info("iter=" + iter + ", indexes=" + toString(nameToMetricMap));
  }

  private String toString(Map<String, Double> metrics){
    StringBuilder sb = new StringBuilder();
    for(Map.Entry<String, Double> entry:metrics.entrySet()) {
      sb.append("index name=").append(entry.getKey()).append(",").append("value=").append(entry.getValue());
    }
    return sb.toString();
  }

  @Override public void handle(MetricsEvent event) {
    if(eventQueue.size() > 10000) {
      LOG.warn("There are over " + 10000 + " event in queue, refuse the new event");
      return;
    }
    eventQueue.add(event);
  }
}
