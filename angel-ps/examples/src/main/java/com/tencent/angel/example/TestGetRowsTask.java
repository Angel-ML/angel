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
import com.tencent.angel.ml.math.TFactory;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.math.vector.TDoubleVector;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.psagent.matrix.transport.adapter.GetRowsResult;
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex;
import com.tencent.angel.worker.WorkerContext;
import com.tencent.angel.worker.task.TaskContext;
import com.tencent.angel.worker.task.TrainTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class TestGetRowsTask extends TrainTask<LongWritable, Text> {
  private static final Log LOG = LogFactory.getLog(TestGetRowsTask.class);

  public TestGetRowsTask(TaskContext taskContext) {
    super(taskContext);
    // TODO Auto-generated constructor stub
  }

  @Override
  public LabeledData parse(LongWritable key, Text value) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void run(TaskContext taskContext) throws Exception {
    int dimension = 1000000;

    int rowNum = 100;

    int iterationNum = 1000;
    double[] data = new double[dimension];
    for (int i = 0; i < data.length; i++) {
      data[i] = 1.0;
    }

    DenseDoubleVector plusVector = new DenseDoubleVector(dimension, data);
    int iterIndex = 0;
    MatrixClient matrixClient = null;
    matrixClient = taskContext.getMatrix(WordCountSubmitter.parameterName);

    RowIndex rowIndexes = new RowIndex();
    for (int i = 0; i < rowNum; i++) {
      rowIndexes.addRowId(i);
    }

    while (iterIndex < iterationNum) {
      GetRowsResult pipelineResult = matrixClient.getRowsFlow(rowIndexes, 10);
      while (true) {
        TDoubleVector weightVector = (TDoubleVector) pipelineResult.take();
        if (weightVector == null) {
          break;
        }

        double[] values = weightVector.getValues();
        double sum = 0.0;
        for (int i = 0; i < values.length; i++) {
          sum += values[i];
        }

        LOG.info("task " + taskContext.getTaskId() + " itertion " + iterIndex + " row index "
            + weightVector.getRowId() + " sum of values is " + sum + ", weight dimension is "
            + weightVector.getDimension());

        DenseDoubleVector grad = TFactory.newDenseDoubleVector(weightVector.getDimension());
        grad.plusBy(plusVector);
        matrixClient.increment(weightVector.getRowId(), grad);
      }
      iterIndex++;
      matrixClient.clock();
    }
  }

}
