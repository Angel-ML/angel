package com.tencent.angel.example.psf;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.update.IncrementRows;
import com.tencent.angel.ml.matrix.psf.update.update.IncrementRowsParam;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.worker.task.BaseTask;
import com.tencent.angel.worker.task.TaskContext;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PytorchPSFTask extends BaseTask<Long, Long, Long> {
  private static final Log LOG = LogFactory.getLog(PytorchPSFTask.class);
  public PytorchPSFTask(TaskContext taskContext) {
    super(taskContext);
  }

  @Override public Long parse(Long key, Long value) {
    return null;
  }

  @Override public void run(TaskContext taskContext) throws AngelException {
    int feaNum = taskContext.getConf().getInt("col", 100000000);
    int batchNNZ = taskContext.getConf().getInt("batch.nnz", 10000);
    int updateTime = 0;
    long startTs = System.currentTimeMillis();
    try {
      while (true) {
        int[] indices = genIndexs(feaNum, batchNNZ);
        IntFloatVector deltaVec = VFactory.sparseFloatVector(feaNum, batchNNZ);
        for(int i = 0; i < indices.length; i++) {
          deltaVec.set(indices[i], 1);
        }
        Vector[] updates = new Vector[1];
        updates[0] = deltaVec;
        MatrixClient client = taskContext.getMatrix("psf_test");
        client.asyncUpdate(new IncrementRows(new IncrementRowsParam(client.getMatrixId(), updates))).get();
        updateTime++;
        if(updateTime % 100 == 0) {
          LOG.info("update num = " + updateTime + ", avg update time=" + (System.currentTimeMillis() - startTs) / updateTime);
        }
      }
    } catch (Throwable ie) {

    }
  }

  @Override public void preProcess(TaskContext taskContext) {

  }

  public static int[] genIndexs(int feaNum, int nnz) {

    int[] sortedIndex = new int[nnz];
    Random random = new Random(System.currentTimeMillis());
    sortedIndex[0] = random.nextInt(feaNum / nnz);
    for (int i = 1; i < nnz; i++) {
      int rand = random.nextInt((feaNum - sortedIndex[i - 1]) / (nnz - i));
      if (rand == 0) {
        rand = 1;
      }
      sortedIndex[i] = rand + sortedIndex[i - 1];
    }

    return sortedIndex;
  }
}
