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

import com.tencent.angel.ml.feature.LabeledData;
import com.tencent.angel.worker.task.TaskContext;
import com.tencent.angel.worker.task.TrainTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class TestUpdaterAndAggrTask extends TrainTask<LongWritable, Text> {
  public TestUpdaterAndAggrTask(TaskContext taskContext) throws IOException {
    super(taskContext);
  }

  private static final Log LOG = LogFactory.getLog(TestUpdaterAndAggrTask.class);

  @Override
  public void run(TaskContext taskContext) throws Exception {
//
//    int dimension =
//        WorkerContext
//            .get()
//            .getConf()
//            .getInt(AngelConfiguration.ANGEL_PREPROCESS_VECTOR_MAXDIM,
//                AngelConfiguration.DEFAULT_ANGEL_PREPROCESS_VECTOR_MAXDIM);
//
//    int iterationNum = 1000;
//    double[] delta = new double[dimension];
//    for (int i = 0; i < delta.length; i++) {
//      delta[i] = 1.0;
//    }
//
//    // DenseDoubleVector plusVector = new DenseDoubleVector(dimension, data);
//    int iterIndex = 0;
//    // MatrixClient matrixClient = null;
//    // matrixClient = taskContext.getMatrix(WordCountSubmitter.parameterName);
//
//    Set<Integer> ids = PSAgentContext.get().getMatrixMetaManager().getMatrixIds();
//
//
//
//    while (iterIndex < iterationNum) {
//      for (int matrixId : ids) {
//        MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
//        MatrixClient client = MatrixClientFactory.get(matrixId, taskContext.getTaskId().getIndex());
//
//        if (iterIndex == 0) {
//          DenseDoubleVector row = (DenseDoubleVector) client.getRow(0);
//          LOG.debug("matrix name=" + meta.getName() + ", rowIndex=0, sum=" + sum(row.getValues()));
//
//          DenseDoubleVector deltaRow = new DenseDoubleVector(delta.length, delta);
//          deltaRow.setMatrixId(matrixId);
//          deltaRow.setRowId(0);
//          client.increment(deltaRow);
//          client.clock();
//
//          Thread.sleep(5000);
//        }
//
//        SumAggrParam aggrParam = new SumAggrParam(matrixId);
//        SumAggrFunc aggrFunc = new SumAggrFunc(aggrParam);
//
//        ScalarUpdaterParam updaterParam = new ScalarUpdaterParam(matrixId, false, 2.0);
//        ScalarUpdater updaterFunc = new ScalarUpdater(updaterParam);
//
//
//        Future<VoidResult> futureResult = client.update(updaterFunc);
//        futureResult.get();
//
//        SumAggrResult aggrResult = (SumAggrResult) (client.aggr(aggrFunc));
//        LOG.debug("aggr sum=" + aggrResult.getResult());
//
//        LOG.debug("matrix name=" + meta.getName() + " clocked!");
//      }
//
//      iterIndex++;
//    }
//
//    /*
//     * int [] data = new int[dimension]; for(int i = 0; i < data.length;){ data[i] = 1; i++; }
//     *
//     * DenseIntVector plusVector = new DenseIntVector(dimension, data); int iterIndex = 0; while
//     * (iterIndex < iterationNum){ TIntVector weightVector = (TIntVector)
//     * taskContext.getRow(WorkerCounterSubmitter.parameterName, 0);
//     *
//     * int[] values = weightVector.getValues(); long sum = 0; for(int i = 0; i < values.length;
//     * i++){ sum += values[i]; }
//     *
//     * LOG.info("task " + taskContext.getTaskId() + "itertion " + iterIndex + " sum of values is " +
//     * sum + ", weight dimension is " + weightVector.getDimension());
//     *
//     * DenseIntVector grad = TFactory.newDenseIntVector(weightVector.getDimension());
//     * grad.plusBy(plusVector); int [] vvs = grad.getValues(); for(int i = 0; i < 10; i++){
//     * LOG.debug("vvs[" + i + "]=" + vvs[i]); }
//     *
//     * taskContext.increment(weightVector.getMatrixId(), weightVector.getRowId(), grad);
//     * taskContext.clockAndWait(); iterIndex++; }
//     */

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
