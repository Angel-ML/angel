package com.tencent.angel.ml.math.executor;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.psagent.PSAgentContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.*;

/**
 * Matrix operation executor. It use ForkJoin to achieve parallel computing.
 */
public class MatrixOpExecutors {
  private static final Log LOG = LogFactory.getLog(MatrixOpExecutors.class);
  /**
   * Worker pool for forkjoin
   */
  private static final ForkJoinPool pool = new ForkJoinPool(PSAgentContext.get().getConf()
    .getInt(AngelConf.ANGEL_WORKER_MATRIX_EXECUTORS_NUM,
      AngelConf.DEFAULT_ANGEL_WORKER_MATRIX_EXECUTORS_NUM));

  /**
   * Execute a task use ForkJoin
   *
   * @param task a implementation of ForkJoinTask
   */
  public static void execute(ForkJoinTask task) {
    pool.execute(task);
  }
}
