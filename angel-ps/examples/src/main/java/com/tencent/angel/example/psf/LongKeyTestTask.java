package com.tencent.angel.example.psf;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.CompSparseDoubleLongKeyVector;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.math.vector.SparseDoubleLongKeyVector;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.worker.task.BaseTask;
import com.tencent.angel.worker.task.TaskContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LongKeyTestTask extends BaseTask<Long, Long, Long> {
  private static final Log LOG = LogFactory.getLog(PSFTestTask.class);

  public LongKeyTestTask(TaskContext taskContext) {
    super(taskContext);
  }

  @Override public Long parse(Long key, Long value) {
    return null;
  }

  @Override public void preProcess(TaskContext taskContext) { }

  @Override public void run(TaskContext taskContext) throws AngelException {
    try{
      MatrixClient client = taskContext.getMatrix("longkey_test");
      while (taskContext.getEpoch() < 100) {
        long startTs = System.currentTimeMillis();
        TVector row = client.getRow(0);
        LOG.info("Task " + taskContext.getTaskId() + " in iteration " + taskContext.getEpoch()
          + " pull use time=" + (System.currentTimeMillis() - startTs) + ", sum=" + ((CompSparseDoubleLongKeyVector)row).sum());

        startTs = System.currentTimeMillis();
        CompSparseDoubleLongKeyVector
          deltaV = new CompSparseDoubleLongKeyVector(client.getMatrixId(), 0,2100000000, 110000000);
        SparseDoubleLongKeyVector deltaV1 = new SparseDoubleLongKeyVector(2100000000, 150000000);
        DenseDoubleVector deltaV2 = new DenseDoubleVector(110000000);
        for(int i = 0; i < 2100000000; i+=20) {
          deltaV.set(i, 1.0);
          deltaV1.set(i, 1.0);
        }

        for(int i = 0; i < 110000000; i++) {
          deltaV2.set(i, 1.0);
        }

        startTs = System.currentTimeMillis();
        int tryNum = 100;
        while(tryNum-- > 0) {
          deltaV.timesBy(2.0);
        }

        LOG.info("combine times use time " + (System.currentTimeMillis() - startTs));

        startTs = System.currentTimeMillis();
        tryNum = 100;
        while(tryNum-- > 0) {
          deltaV1.timesBy(2.0);
        }

        LOG.info("single times use time " + (System.currentTimeMillis() - startTs));

        startTs = System.currentTimeMillis();
        tryNum = 100;
        while(tryNum-- > 0) {
          deltaV2.timesBy(2.0);
        }

        LOG.info("dense times use time " + (System.currentTimeMillis() - startTs));

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

  private double sum(SparseDoubleLongKeyVector row) {
    double [] data = row.getValues();
    double ret = 0.0;
    for(int i = 0; i < data.length; i++) {
      ret += data[i];
    }

    return ret;
  }
}
