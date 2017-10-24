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
import com.tencent.angel.ml.math.vector.CompSparseLongKeyDoubleVector;
import com.tencent.angel.ml.math.vector.DenseIntDoubleVector;
import com.tencent.angel.ml.math.vector.SparseLongKeyDoubleVector;
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
          + " pull use time=" + (System.currentTimeMillis() - startTs) + ", sum=" + ((CompSparseLongKeyDoubleVector)row).sum());

        startTs = System.currentTimeMillis();
        CompSparseLongKeyDoubleVector
          deltaV = new CompSparseLongKeyDoubleVector(client.getMatrixId(), 0,2100000000, 110000000);
        SparseLongKeyDoubleVector deltaV1 = new SparseLongKeyDoubleVector(2100000000, 150000000);
        DenseIntDoubleVector deltaV2 = new DenseIntDoubleVector(110000000);
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

  private double sum(SparseLongKeyDoubleVector row) {
    double [] data = row.getValues();
    double ret = 0.0;
    for(int i = 0; i < data.length; i++) {
      ret += data[i];
    }

    return ret;
  }
}
