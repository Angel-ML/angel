package com.tencent.angel.ps.metrics;


import com.google.common.collect.ImmutableMap;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.metrics.AngelMetricStatus;
import com.tencent.angel.metrics.AngelMetricsDimensionReporter;
import com.tencent.angel.ps.PSContext;
import java.lang.reflect.Constructor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Periodically reports memory and cpu usage of each ps through a pluggable
 * {@link AngelMetricsDimensionReporter} configured by
 * {@link AngelConf#ANGEL_METRICS_ADS_REPORTER}. If no reporter is configured (the default), the
 * service does nothing.
 */
public class AngelPSAdsMetricsService {
  private static final Log LOG = LogFactory.getLog(AngelPSAdsMetricsService.class);

  private final Configuration conf;
  private final AdsPSMetricThread handle;
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private final long reportIntervalMs;
  private final String modelName;
  private final AngelMetricsDimensionReporter reporter;
  private int applyMemoryMb;
  private int applyCores;
  private String host;

  public AngelPSAdsMetricsService(PSContext context) {
    this.conf = context.getConf();
    this.reportIntervalMs = conf.getLong(
        AngelConf.ANGEL_METRICS_ADS_INTERVAL_PERIOD_MILLISECOND,
        AngelConf.DEFAULT_ANGEL_ADS_METRICS_INTERVAL_PERIOD_MILLISECOND);
    this.modelName = conf.get(AngelConf.ANGEL_METRICS_ADS_MODEL_NAME,
        AngelConf.DEFAULT_ANGEL_METRICS_ADS_MODEL_NAME);
    try {
      this.host = context.getPs().getHostAddress();
    } catch (Exception e) {
      this.host = "unknown";
    }

    this.applyMemoryMb = conf.getInt(AngelConf.ANGEL_PS_MEMORY_GB,
        AngelConf.DEFAULT_ANGEL_PS_MEMORY_GB) * 1024;
    if (this.applyMemoryMb < 2048) {
      this.applyMemoryMb = 2048;
    }

    this.applyCores = conf.getInt(AngelConf.ANGEL_PS_CPU_VCORES, AngelConf.DEFAULT_ANGEL_PS_CPU_VCORES);

    this.reporter = createReporter(conf.get(AngelConf.ANGEL_METRICS_ADS_REPORTER,
        AngelConf.DEFAULT_ANGEL_METRICS_ADS_REPORTER));

    handle = new AdsPSMetricThread();
  }

  /**
   * Instantiate the dimension reporter. A dedicated instance is created on purpose: the shared
   * singleton in MetricsUtils holds the generic metrics reporter, which is a different type.
   */
  private static AngelMetricsDimensionReporter createReporter(String className) {
    if (className == null || className.trim().isEmpty()) {
      LOG.warn(AngelConf.ANGEL_METRICS_ADS_REPORTER
          + " is not set, ps resource metrics will not be reported.");
      return null;
    }
    try {
      Class<?> clazz = Class.forName(className.trim());
      if (!AngelMetricsDimensionReporter.class.isAssignableFrom(clazz)) {
        LOG.error(className + " is not a subclass of "
            + AngelMetricsDimensionReporter.class.getName()
            + ", ps resource metrics will not be reported.");
        return null;
      }
      Constructor<?> cons = clazz.getDeclaredConstructor();
      cons.setAccessible(true);
      return (AngelMetricsDimensionReporter) cons.newInstance();
    } catch (Throwable e) {
      LOG.error("Create ps resource metrics reporter " + className
          + " failed, ps resource metrics will not be reported.", e);
      return null;
    }
  }

  public void start() {
    if (reporter == null) {
      return;
    }
    handle.setDaemon(true);
    handle.start();
  }

  public void stop() {
    if (stopped.compareAndSet(false, true)) {
      handle.interrupt();
      if (reporter != null) {
        try {
          reporter.close();
        } catch (Throwable e) {
          LOG.warn("Close ps resource metrics reporter failed", e);
        }
      }
    }
  }

  public static String memoryMb() {
    Runtime runTime = Runtime.getRuntime();
    float memory = (runTime.totalMemory() - runTime.freeMemory()) / 1024.0f / 1024.0f ;
    return String.valueOf(memory);
  }

  private void reportPSMetrics() {
    reporter.report(
        ImmutableMap.of("task_name", host, "model_name", modelName),
        ImmutableMap.of(
        "psCPU_apply", String.valueOf(applyCores),
        "psCPU_use", String.valueOf(applyCores),
        "psMEM_apply", String.valueOf(applyMemoryMb),
        "psMEM_use", memoryMb()), AngelMetricStatus.SUM);
  }

  class AdsPSMetricThread extends Thread {

    @Override
    public void run() {
      while (!stopped.get() && !Thread.interrupted()) {
        try {
          reportPSMetrics();
        } catch (Throwable e) {
          LOG.warn("Report ps resource metrics failed", e);
        }
        try {
          Thread.sleep(reportIntervalMs);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
  }
}
