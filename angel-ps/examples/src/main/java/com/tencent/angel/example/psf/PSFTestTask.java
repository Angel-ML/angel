/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
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
import com.tencent.angel.ml.math.vector.SparseLongKeyDoubleVector;
import com.tencent.angel.ml.matrix.psf.get.enhance.indexed.LongIndexGetFunc;
import com.tencent.angel.ml.matrix.psf.get.enhance.indexed.LongIndexGetParam;
import com.tencent.angel.ml.matrix.psf.get.single.GetRowResult;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.worker.task.BaseTask;
import com.tencent.angel.worker.task.TaskContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Random;

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
    long col = conf.getLong("col", 1000000);
    int len = conf.getInt("len", 500000);
    try{
      MatrixClient client = taskContext.getMatrix("psf_test");
      int exeTime = 1000000000;
      long pullTime = 0L;
      long pushTime = 0L;

      for(int time = 0; time < exeTime; time++) {
        long startTs = System.currentTimeMillis();
        long[] indexes = generateIndexes(col, len);
        LOG.info("start to get values");
        LongIndexGetFunc
          func = new LongIndexGetFunc(new LongIndexGetParam(client.getMatrixId(), 0, indexes));
        TVector row = ((GetRowResult) client.get(func)).getRow();
        LOG.info("after to get values");
        pullTime += (System.currentTimeMillis() - startTs);
        if(time % 1 == 0) {
          LOG.info("Task " + taskContext.getTaskId() + " in iteration " + taskContext.getEpoch()
            + " pull use time=" + (pullTime / 1) + ", sum of row 0=" + sum((SparseLongKeyDoubleVector)row));
          pullTime = 0;
        }

        double [] delta = new double[len];
        for(int i = 0; i < len; i++) {
          delta[i] = 1.0;
        }
        SparseLongKeyDoubleVector deltaV = new SparseLongKeyDoubleVector(col, indexes, delta);
        deltaV.setMatrixId(client.getMatrixId());
        deltaV.setRowId(0);

        startTs = System.currentTimeMillis();
        client.increment(deltaV);
        client.clock().get();
        pushTime += (System.currentTimeMillis() - startTs);

        if(time % 1 == 0) {
          LOG.info("Task " + taskContext.getTaskId() + " in iteration " + taskContext.getEpoch()
            + " push use time=" + (pushTime / 1));
          pushTime = 0;
          taskContext.incEpoch();
        }

        Thread.sleep(10000);
      }
    } catch (Throwable x) {
      throw new AngelException("run task failed ", x);
    }
  }

  private long [] generateIndexes(long range, int size) {
    Random r = new Random();
    long [] result = new long[size];
    for(int i = 0; i < size; i++) {
      result[i] = Math.abs(r.nextLong()) % range;
    }
    return result;
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
