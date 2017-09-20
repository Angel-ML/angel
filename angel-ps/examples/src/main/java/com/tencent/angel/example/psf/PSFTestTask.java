package com.tencent.angel.example.psf;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.matrix.psf.aggr.primitive.Pull;
import com.tencent.angel.ml.matrix.psf.get.single.GetRowResult;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.worker.task.BaseTask;
import com.tencent.angel.worker.task.TaskContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created by payniexiao on 2017/7/18.
 */
public class PSFTestTask extends BaseTask<Long, Long, Long> {
  private static final Log LOG = LogFactory.getLog(PSFTestTask.class);

  public PSFTestTask(TaskContext taskContext) {
    super(taskContext);
  }

  @Override public Long parse(Long key, Long value) {
    return null;
  }

  @Override public void preProcess(TaskContext taskContext) { }

  @Override public void run(TaskContext taskContext) throws AngelException {
    try{
      MatrixClient client = taskContext.getMatrix("psf_test");
      Pull func = new Pull(client.getMatrixId(), 0);
      double [] delta = new double[100000000];
      for(int i = 0; i < 100000000; i++) {
        delta[i] = 1.0;
      }
      while (taskContext.getEpoch() < 1000) {
        taskContext.globalSync(client.getMatrixId());
        long startTs = System.currentTimeMillis();
        TVector row = ((GetRowResult) client.get(func)).getRow();
        LOG.info("Task " + taskContext.getTaskId() + " in iteration " + taskContext.getEpoch()
          + " pull use time=" + (System.currentTimeMillis() - startTs) + ", sum=" + sum((DenseDoubleVector)row));

        DenseDoubleVector deltaV = new DenseDoubleVector(100000000, delta);
        deltaV.setMatrixId(client.getMatrixId());
        deltaV.setRowId(0);

        client.increment(deltaV);
        client.clock().get();
        taskContext.incEpoch();
      }
    } catch (Throwable x) {
      throw new AngelException("run task failed ", x);
    }
  }

  private double sum(DenseDoubleVector row) {
    double [] data = row.getValues();
    double ret = 0.0;
    for(int i = 0; i < data.length; i++) {
      ret += data[i];
    }

    return ret;
  }
}
