/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.example.psf;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.matrix.psf.aggr.Pull;
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
    int col = conf.getInt("col", 100000000);
    try{
      MatrixClient client = taskContext.getMatrix("psf_test");
      Pull func = new Pull(client.getMatrixId(), 0);
      //Pull func1 = new Pull(client.getMatrixId(), 1);

      while (taskContext.getEpoch() < 100) {
        //taskContext.globalSync(client.getMatrixId());
        long startTs = System.currentTimeMillis();
        TVector row = ((GetRowResult) client.get(func)).getRow();//client.getRow(0);
        //TVector row1 = ((GetRowResult) client.get(func1)).getRow();
        LOG.info("Task " + taskContext.getTaskId() + " in iteration " + taskContext.getEpoch()
          + " pull use time=" + (System.currentTimeMillis() - startTs) + ", sum of row 0=" + sum((DenseDoubleVector)row));

        double [] delta = new double[col];
        for(int i = 0; i < col; i++) {
          delta[i] = 1.0;
        }
        DenseDoubleVector deltaV = new DenseDoubleVector(col, delta);
        deltaV.setMatrixId(client.getMatrixId());
        deltaV.setRowId(0);

        //double [] delta1 = new double[col];
        //for(int i = 0; i < col; i++) {
        //  delta1[i] = 2.0;
        //}
        //DenseDoubleVector deltaV1 = new DenseDoubleVector(col, delta1);
        //deltaV1.setMatrixId(client.getMatrixId());
        //deltaV1.setRowId(1);

        client.increment(deltaV);
        //client.increment(deltaV1);
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
