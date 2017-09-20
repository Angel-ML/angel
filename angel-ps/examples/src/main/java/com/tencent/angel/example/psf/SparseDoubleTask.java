package com.tencent.angel.example.psf;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.SparseDoubleVector;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.worker.task.BaseTask;
import com.tencent.angel.worker.task.TaskContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SparseDoubleTask extends BaseTask<Long, Long, Long> {
  private static final Log LOG = LogFactory.getLog(PSFTestTask.class);
  public SparseDoubleTask(TaskContext taskContext) {
    super(taskContext);
  }

  @Override public Long parse(Long key, Long value) {
    return null;
  }

  @Override public void preProcess(TaskContext taskContext) { }


  @Override public void run(TaskContext taskContext) throws AngelException {
    try{
      MatrixClient client = taskContext.getMatrix("sparse_double_test");
      while (taskContext.getEpoch() < 100) {
        long startTs = System.currentTimeMillis();
        TVector row = client.getRow(0);
        LOG.info("Task " + taskContext.getTaskId() + " in iteration " + taskContext.getEpoch()
          + " pull use time=" + (System.currentTimeMillis() - startTs) + ", sum=" + sum((SparseDoubleVector)row));

        startTs = System.currentTimeMillis();
        SparseDoubleVector deltaV = new SparseDoubleVector(2100000000, 150000000);
        for(int i = 0; i < 2100000000; i+=20) {
          deltaV.set(i, 1.0);
        }

        deltaV.setMatrixId(client.getMatrixId());
        deltaV.setRowId(0);

        LOG.info("Task " + taskContext.getTaskId() + " in iteration " + taskContext.getEpoch()
          + " train use time=" + (System.currentTimeMillis() - startTs));

        startTs = System.currentTimeMillis();
        client.increment(deltaV);
        client.clock().get();
        LOG.info("Task " + taskContext.getTaskId() + " in iteration " + taskContext.getEpoch()
          + " flush use time=" + (System.currentTimeMillis() - startTs));
        taskContext.incEpoch();
      }
    } catch (Throwable x) {
      throw new AngelException("run task failed ", x);
    }
  }

  private double sum(SparseDoubleVector row) {
    double [] data = row.getValues();
    double ret = 0.0;
    for(int i = 0; i < data.length; i++) {
      ret += data[i];
    }

    return ret;
  }
}
