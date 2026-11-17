package com.tencent.angel.metrics;

import com.tencent.angel.exception.AngelException;
import java.lang.reflect.Constructor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MetricsUtils {

  static final Log LOG = LogFactory.getLog(MetricsUtils.class);

  private volatile static AngelMetricsReporter reporter;

  public static AngelMetricsReporter getOrCreateReporter(String reporterName) {
    if (reporter == null) {
      synchronized (AngelMetricsReporter.class) {
        if (reporter == null) {
          try {
            Class clazz = Class.forName(reporterName);
            Constructor[] cons = clazz.getDeclaredConstructors();
            for (int i = 0; i < cons.length; i++) {
              Class<?> clazzs[] = cons[i].getParameterTypes();
              //use private constructor of Singleton Reporter
              if (clazzs.length == 0) {
                cons[i].setAccessible(true);
                reporter = (AngelMetricsReporter) (cons[i].newInstance());
                break;
              }
            }
          } catch (Throwable e) {
            LOG.error("Init metrics resport failed ", e);
            throw new AngelException(e);
          }
        }
      }
    }
    return reporter;
  }

  /**
   * get containerId by parse jvm parameter
   */
  public static String getContainerId() {
    String tmpdir = System.getProperty("java.io.tmpdir");
    String[] items = tmpdir.trim().split("/");
    if (items != null && items.length > 2) {
      return items[items.length - 2];
    }
    return "unknown";
  }

}
