package com.tencent.angel.ps.metrics;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.metrics.AngelMetrics;
import com.tencent.angel.metrics.AngelMetricsReporter;
import com.tencent.angel.metrics.AngelPSLoad;
import com.tencent.angel.metrics.AngelPSLoadMetrics;
import com.tencent.angel.metrics.MetricsItem;
import com.tencent.angel.metrics.MetricsUtils;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.io.PSModelIOExecutor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AngelPSMetricsService {

  private final static Log LOG = LogFactory.getLog(PSModelIOExecutor.class);

  private final PSContext psContext;

  private final AtomicBoolean stopped = new AtomicBoolean(false);
  /**
   * Is collect metric
   */
  private final boolean metricEnable;

  private final String sep;

  private final int reportIntervalMs;

  /**
   * Report buffer len message to NGCP
   */
  private final MetricsReporter metricsReporter;

  /**
   * Store funcName to psfBufferLen
   */
  private volatile ConcurrentHashMap<String, AngelMetrics> keyToMetricsMap = new ConcurrentHashMap<>();

  public AngelPSMetricsService(PSContext psContext) {
    this.psContext = psContext;
    this.metricEnable = psContext.getMetricsEnable();
    this.sep = psContext.getMetricsTextSep();
    this.reportIntervalMs = psContext.getMetricsReportIntervalMs();
    this.metricsReporter = new MetricsReporter();
  }

  public void init() {

  }

  public void start() {
    if (metricEnable) {
      metricsReporter.start();
    }
  }

  public void stop() {
    if (metricEnable && stopped.compareAndSet(false, true)) {
      metricsReporter.sendMessage();
      metricsReporter.interrupt();
    }
  }

  public void report(MetricsItem metrics) {
    if (metricEnable) {
      String key = metrics.generateKey(sep);
      AngelMetrics mergedMetrics = keyToMetricsMap.get(key);
      if (mergedMetrics == null) {
        mergedMetrics = keyToMetricsMap.putIfAbsent(key, createPSLoadMetrics(metrics));
        if (mergedMetrics != null) {
          mergedMetrics.merge(metrics);
        }
      } else {
        mergedMetrics.merge(metrics);
      }
    }
  }

  private AngelPSLoadMetrics createPSLoadMetrics(MetricsItem metrics) {
    return new AngelPSLoadMetrics(MetricsUtils.getContainerId(),
        psContext.getRunningContext().getTotalRPCCounter(),
        psContext.getRunningContext().getState().toString(), (AngelPSLoad) metrics);
  }

  /**
   * Report buffer len message to NGCP
   */
  class MetricsReporter extends Thread {

    String tid = psContext.getConf().get(AngelConf.ANGEL_METRICS_TID_ANGEL_PS_LOAD,
        AngelConf.DEFAULT_ANGEL_METRICS_TID_ANGEL_PS_LOAD);

    AngelMetricsReporter reporter;

    @Override
    public void run() {

      try {
        reporter = MetricsUtils.getOrCreateReporter(
            psContext.getConf().get(AngelConf.ANGEL_METRICS_REPORTER,
                AngelConf.DEFAULT_ANGEL_METRICS_REPORTER));
      } catch (Throwable e) {
        LOG.error("Can not init reporter ", e);
        return;
      }

      while (!stopped.get() && !Thread.interrupted()) {
        try {
          Thread.sleep(reportIntervalMs);

          sendMessage();

        } catch (InterruptedException e) {
          if (!stopped.get()) {
            LOG.error("Reporter is interrupt ", e);
          }
        } catch (Throwable e) {
          LOG.error("Reporter is failed ", e);
        }
      }
    }

    public void sendMessage() {
      // Switch
      ConcurrentHashMap<String, AngelMetrics> oldMetrics = keyToMetricsMap;
      keyToMetricsMap = new ConcurrentHashMap<>(oldMetrics.size());

      List<String> metrics = new ArrayList<>();
      for (Map.Entry<String, AngelMetrics> entry : oldMetrics.entrySet()) {
        metrics.add(entry.getValue().generateTextMessage(sep));
      }

      if (metrics.size() > 0) {
        reporter.asyncSend(metrics, psContext.getConf(), tid);
      }
    }
  }
}
