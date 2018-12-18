package com.tencent.angel.ml.math2;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Matrix operation parallel executor, it use a fork-join pool and can use
 * "angel.math.matrix.op.parallel.worker.num" to set parallelism
 */
public class MatrixExecutors {

  private static final Log LOG = LogFactory.getLog(MatrixExecutors.class);
  /**
   * Matrix executor instance
   */
  private static MatrixExecutors instance;

  /**
   * Worker pool of the executor
   */
  private final ForkJoinPool pool;

  public static final String WORKER_NUM_PARAMETER_NAME = "angel.math.matrix.op.parallel.worker.num";

  static {

  }

  private MatrixExecutors(int poolSize) {
    pool = new ForkJoinPool(poolSize);
  }

  /**
   * Get matrix executor instance
   *
   * @return matrix executor instance
   */
  public synchronized static MatrixExecutors getInstance() {
    if(instance == null) {
      int poolSize;
      int defaultPoolSize = Runtime.getRuntime().availableProcessors();
      String numStr = System.getProperty(WORKER_NUM_PARAMETER_NAME);
      if (numStr == null) {
        poolSize = defaultPoolSize;
        LOG.warn(
            WORKER_NUM_PARAMETER_NAME + " is not set, just use default worker number:" + poolSize);
      } else {
        try {
          poolSize = Integer.valueOf(numStr);
          if (poolSize <= 0) {
            poolSize = defaultPoolSize;
            LOG.error(WORKER_NUM_PARAMETER_NAME + " value " + numStr
                + " is not a valid value(must be a integer > 0), just use default value "
                + defaultPoolSize);
          }
        } catch (Throwable x) {
          poolSize = defaultPoolSize;
          LOG.error(WORKER_NUM_PARAMETER_NAME + " value " + numStr
              + " is not a valid value(must be a integer > 0), just use default value "
              + defaultPoolSize);
        }
      }

      instance = new MatrixExecutors(poolSize);
    }
    return instance;
  }

  /**
   * Execute a task use ForkJoin
   *
   * @param task a implementation of ForkJoinTask
   */
  public void execute(ForkJoinTask task) {
    pool.execute(task);
  }

  /**
   * Execute a task
   *
   * @param task a implementation of Runnable
   */
  public void execute(Runnable task) {
    pool.execute(task);
  }

  /**
   * Get the workers number
   *
   * @return workers number
   */
  public int getParallel() {
    return pool.getParallelism();
  }

  /**
   * Shut down all workers
   */
  public void shutdown() {
    if (pool != null) {
      pool.shutdownNow();
    }
  }
}
