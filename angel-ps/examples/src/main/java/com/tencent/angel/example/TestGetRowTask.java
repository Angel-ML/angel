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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.example;

import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.ml.feature.LabeledData;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.math.vector.TDoubleVector;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.udf.aggr.SumAggrFunc;
import com.tencent.angel.ml.matrix.udf.aggr.SumAggrFunc.SumAggrParam;
import com.tencent.angel.ml.matrix.udf.aggr.SumAggrFunc.SumAggrResult;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.psagent.matrix.MatrixClientFactory;
import com.tencent.angel.worker.WorkerContext;
import com.tencent.angel.worker.task.TaskContext;
import com.tencent.angel.worker.task.TrainTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Set;

public class TestGetRowTask extends TrainTask<LongWritable, Text> {
  public TestGetRowTask(TaskContext taskContext) throws IOException {
    super(taskContext);
  }

  private static final Log LOG = LogFactory.getLog(TestGetRowTask.class);

  @Override
  public void run(TaskContext taskContext) throws Exception {

    int dimension = 1000000;

    int iterationNum = 10;
    double[] delta = new double[dimension];
    for (int i = 0; i < delta.length; i++) {
      delta[i] = 1.0;
    }

    Set<Integer> ids = PSAgentContext.get().getMatrixMetaManager().getMatrixIds();
    while (taskContext.getIteration() < iterationNum) {
      LOG.info("taskContext=" + taskContext);
      for (int matrixId : ids) {
        MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
        MatrixClient client = MatrixClientFactory.get(matrixId, taskContext.getTaskId().getIndex());
        TDoubleVector row = (TDoubleVector) client.getRow(0);

        double sum = sum(row.getValues());

        SumAggrParam aggrParam = new SumAggrParam(matrixId);
        SumAggrFunc aggrFunc = new SumAggrFunc(aggrParam);
        SumAggrResult aggrResult = (SumAggrResult) (client.aggr(aggrFunc));

        LOG.info("taskid=" + taskContext.getTaskIndex() + ", matrixId=" + matrixId
            + ", rowIndex=0, local row sum=" + sum + ", aggr sum=" + aggrResult.getResult());

        DenseDoubleVector deltaRow = new DenseDoubleVector(delta.length, delta);
        deltaRow.setMatrixId(matrixId);
        deltaRow.setRowId(0);
        row.plusBy(deltaRow);
        client.increment(deltaRow);
        client.clock().get();

        LOG.info("matrix name=" + meta.getName() + " clocked!");
      }

      taskContext.increaseIteration();
    }


    int tryNum = 5;
    while (tryNum-- > 0) {
      for (int matrixId : ids) {
        MatrixClient client = MatrixClientFactory.get(matrixId, taskContext.getTaskId().getIndex());
        TDoubleVector row = (TDoubleVector) client.getRow(0);
        double sum = sum(row.getValues());

        SumAggrParam aggrParam = new SumAggrParam(matrixId);
        SumAggrFunc aggrFunc = new SumAggrFunc(aggrParam);
        SumAggrResult aggrResult = (SumAggrResult) (client.aggr(aggrFunc));

        LOG.info("task interation end, taskid=" + taskContext.getTaskIndex() + ", matrixId="
            + matrixId + ", rowIndex=0, local row sum=" + sum + ", aggr sum="
            + aggrResult.getResult());
      }

      Thread.sleep(1000);
    }
  }

  public double sum(double[] args) {
    double sum = 0.0;
    for (int i = 0; i < args.length; i++) {
      sum += args[i];
    }
    return sum;
  }


  @Override
  public LabeledData parse(LongWritable key, Text value) {
    // TODO Auto-generated method stub
    return null;
  }
}
