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

package com.tencent.angel.ml.algorithm.regression.LinearRegressionWithADMM;

import com.tencent.angel.ml.feature.LabeledData;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.math.vector.SparseDummyVector;
import com.tencent.angel.ml.math.vector.TDoubleVector;
import com.tencent.angel.worker.task.TaskContext;
import com.tencent.angel.worker.task.TrainTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;

class ADMMLinearRegressionTask extends TrainTask<LongWritable, Text> {

  public ADMMLinearRegressionTask(TaskContext taskContext) throws IOException {
    super(taskContext);
    // TODO Auto-generated constructor stub
  }

  private final static Log LOG = LogFactory.getLog(ADMMLinearRegressionTask.class);

  public void run(TaskContext context) throws Exception {

    ArrayList<LabeledData> instances = new ArrayList<LabeledData>();

    while (true) {
      LabeledData data = trainDataStorage.read();
      if (data == null) {
        break;
      }
      instances.add(data);
    }

    Configuration conf = context.getConf();

    double l1Norm = conf.getDouble(ADMMLinearRegressionSubmitter.K_L1_NORM, 5);
    int admmIterNum = conf.getInt(ADMMLinearRegressionSubmitter.K_ADMM_ITER, 10);
    int lbfgsIterNum = conf.getInt(ADMMLinearRegressionSubmitter.K_LBFGS_ITER, 5);
    double rho = conf.getDouble(ADMMLinearRegressionSubmitter.K_RHO, 10);
    int feaNum = conf.getInt(ADMMLinearRegressionSubmitter.K_FEATURE_NUM, 10000000);
    int N = context.getTotalTaskNum();

    LOG.info("L1NORM=" + l1Norm);
    LOG.info("ADMM_ITER_NUM=" + admmIterNum);
    LOG.info("LBGFS_ITER_NUM=" + lbfgsIterNum);
    LOG.info("RHO=" + rho);
    LOG.info("N=" + N);

    ADMMLinearRegressionState state = new ADMMLinearRegressionState(instances);

    ADMMLinearRegressionOptimizer optimizer = new ADMMLinearRegressionOptimizer(feaNum, context);

    TDoubleVector z = optimizer.train(l1Norm, rho, N, feaNum, admmIterNum, lbfgsIterNum, state);

    RegressionEvaluate.accuracy(state, (DenseDoubleVector) z);
  }

  @Override
  public LabeledData parse(LongWritable key, Text value) {
    if (value == null)
      return null;

    String[] splits = value.toString().split(",");

    if (splits.length < 1) {
      return null;
    }

    SparseDummyVector x = new SparseDummyVector(-1, splits.length - 1);

    double y = Double.parseDouble(splits[0]);

    for (int i = 1; i < splits.length; i++) {
      int index = Integer.parseInt(splits[i]);
      x.set(index, 1.0);
    }

    LabeledData labeledData = new LabeledData(x, y);
    return labeledData;
  }

}
