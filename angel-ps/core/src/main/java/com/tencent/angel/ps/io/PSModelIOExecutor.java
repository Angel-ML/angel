package com.tencent.angel.ps.io;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.model.PSMatricesLoadContext;
import com.tencent.angel.model.PSMatricesSaveContext;
import com.tencent.angel.model.PSMatrixLoadContext;
import com.tencent.angel.model.PSMatrixSaveContext;
import com.tencent.angel.model.io.IOExecutors;
import com.tencent.angel.model.output.format.MatrixFormat;
import com.tencent.angel.model.output.format.ModelFilesUtils;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.utils.StringUtils;
import java.io.IOException;
import java.util.Vector;
import java.util.concurrent.RecursiveAction;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * PS Model load/save executors
 */
public class PSModelIOExecutor {

  private final static Log LOG = LogFactory.getLog(PSModelIOExecutor.class);
  /**
   * PS context
   */
  private final PSContext context;

  /**
   * Create PSModelIOExecutor
   *
   * @param context ps context
   */
  public PSModelIOExecutor(PSContext context) {
    this.context = context;
  }

  public void init() {

  }

  public void start() {

  }

  public void stop() {

  }

  /**
   * Load matrices from files
   *
   * @param loadContext load context
   */
  public void load(PSMatricesLoadContext loadContext) throws IOException {
    load(loadContext, context.getConf()
        .getInt(AngelConf.ANGEL_PS_MATRIX_DISKIO_WORKER_POOL_SIZE,
            AngelConf.DEFAULT_ANGEL_PS_MATRIX_DISKIO_WORKER_POOL_SIZE));
  }

  /**
   * Load matrices from files
   *
   * @param loadContext load context
   */
  public void load(PSMatricesLoadContext loadContext, int parallel) throws IOException {
    LOG.info("start to load matrices :" + loadContext);
    Vector<String> errorLogs = new Vector<>();

    // Create and start workers
    IOExecutors workers = createIOExecutors(parallel);
    workers.init();
    workers.start();

    // Set workers
    for (PSMatrixLoadContext matrixLoadContext : loadContext.getMatrixLoadContexts()) {
      matrixLoadContext.setWorkers(workers);
    }

    try {
      MatrixDiskIOOp commitOp = new MatrixDiskIOOp(ACTION.LOAD, errorLogs, loadContext, 0,
          loadContext.getMatrixLoadContexts().size());
      workers.execute(commitOp);
      commitOp.join();
      if (!errorLogs.isEmpty()) {
        throw new IOException(StringUtils.join("\n", errorLogs));
      }
    } catch (Throwable x) {
      throw new IOException(x);
    } finally {
      workers.shutdown();
    }

    return;
  }

  /**
   * Save matrices to files
   *
   * @param saveContext matrices save context
   */
  public void save(PSMatricesSaveContext saveContext) throws IOException {
    save(saveContext, context.getConf()
        .getInt(AngelConf.ANGEL_PS_MATRIX_DISKIO_WORKER_POOL_SIZE,
            AngelConf.DEFAULT_ANGEL_PS_MATRIX_DISKIO_WORKER_POOL_SIZE));
  }


  /**
   * Save matrices to files
   *
   * @param saveContext matrices save context
   */
  public void save(PSMatricesSaveContext saveContext, int parallel) throws IOException {
    LOG.info("start to save matrices :" + saveContext);
    Vector<String> errorLogs = new Vector<>();

    // Create and start workers
    IOExecutors workers = createIOExecutors(parallel);
    workers.init();
    workers.start();

    // Set workers
    for (PSMatrixSaveContext matrixSaveContext : saveContext.getMatrixSaveContexts()) {
      matrixSaveContext.setWorkers(workers);
    }
    try {
      MatrixDiskIOOp commitOp = new MatrixDiskIOOp(ACTION.SAVE, errorLogs, saveContext, 0,
          saveContext.getMatrixSaveContexts().size());
      workers.execute(commitOp);
      commitOp.join();
      if (!errorLogs.isEmpty()) {
        throw new IOException(StringUtils.join("\n", errorLogs));
      }
    } catch (Throwable x) {
      throw new IOException(x);
    } finally {
      workers.shutdown();
    }

    return;
  }

  private IOExecutors createIOExecutors(int parallel) {
    return new IOExecutors(parallel);
  }

  enum ACTION {
    LOAD, SAVE
  }

  class MatrixDiskIOOp extends RecursiveAction {

    private final ACTION action;
    private final Vector<String> errorLogs;
    private final Object context;
    private final int startPos;
    private final int endPos;

    public MatrixDiskIOOp(ACTION action, Vector<String> errorLogs, Object context, int start,
        int end) {
      this.action = action;
      this.errorLogs = errorLogs;
      this.context = context;
      this.startPos = start;
      this.endPos = end;
    }

    @Override
    protected void compute() {
      if (endPos <= startPos) {
        return;
      }

      if (endPos - startPos == 1) {
        try {
          process(context, startPos, action);
        } catch (Throwable e) {
          LOG.error(action + " matrix failed ", e);
          errorLogs.add(action + " matrix failed:" + StringUtils.stringifyException(e));
        }
      } else {
        int middle = (startPos + endPos) / 2;
        MatrixDiskIOOp opLeft = new MatrixDiskIOOp(action, errorLogs, context, startPos, middle);
        MatrixDiskIOOp opRight = new MatrixDiskIOOp(action, errorLogs, context, middle, endPos);
        invokeAll(opLeft, opRight);
      }
    }
  }

  private void process(Object context, int index, ACTION action) throws IOException {
    switch (action) {
      case LOAD:
        loadMatrix(((PSMatricesLoadContext) context).getMatrixLoadContexts().get(index));
        break;

      case SAVE: {
        saveMatrix(
            ((PSMatricesSaveContext) context).getMatrixSaveContexts().get(index));
        break;
      }
    }
  }

  private void loadMatrix(PSMatrixLoadContext loadContext) throws IOException {
    ServerMatrix matrix = context.getMatrixStorageManager().getMatrix(loadContext.getMatrixId());
    if (matrix != null) {
      MatrixFormat format = ModelFilesUtils
          .initFormat(loadContext.getFormatClassName(), context.getConf());
      format.load(matrix, loadContext, context.getConf());
    }
  }

  private void saveMatrix(PSMatrixSaveContext saveContext) throws IOException {
    ServerMatrix matrix = context.getMatrixStorageManager().getMatrix(saveContext.getMatrixId());
    if (matrix != null) {
      MatrixFormat format = ModelFilesUtils
          .initFormat(saveContext.getFormatClassName(), context.getConf());
      format.save(matrix, saveContext, context.getConf());
    }
  }
}
