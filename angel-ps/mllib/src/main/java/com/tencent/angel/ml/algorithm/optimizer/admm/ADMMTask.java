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

package com.tencent.angel.ml.algorithm.optimizer.admm;

import com.tencent.angel.ml.feature.LabeledData;
import com.tencent.angel.ml.math.vector.SparseDummyVector;
import com.tencent.angel.worker.task.TrainTask;
import com.tencent.angel.worker.task.TaskContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;

class ADMMTask extends TrainTask<LongWritable, Text> {
  private final static Log LOG = LogFactory.getLog(ADMMTask.class);

  public ADMMTask(TaskContext taskContext) {
    super(taskContext);
  }

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

    double l1Norm = conf.getDouble(ADMMSubmitter.K_L1_NORM, 5);
    int admmIterNum = conf.getInt(ADMMSubmitter.K_ADMM_ITER, 10);
    int lbfgsIterNum = conf.getInt(ADMMSubmitter.K_LBFGS_ITER, 5);
    double rho = conf.getDouble(ADMMSubmitter.K_RHO, 10);
    int N = context.getTotalTaskNum();

    LOG.info("L1NORM=" + l1Norm);
    LOG.info("ADMM_ITER_NUM=" + admmIterNum);
    LOG.info("LBGFS_ITER_NUM=" + lbfgsIterNum);
    LOG.info("RHO=" + rho);
    LOG.info("N=" + N);

    ADMMState state = new ADMMState(instances);

    int feaNum =
        context.getPSAgent().getMatrixMetaManager().getMatrixMeta(ADMMSubmitter.W).getColNum();
    ADMMSparseLogisticRegressionOptimizer optimizer =
        new ADMMSparseLogisticRegressionOptimizer(feaNum, context);

    optimizer.train(l1Norm, rho, N, feaNum, admmIterNum, lbfgsIterNum, state);
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

    double nonclick = 0;
    double click = 0;
    if (Integer.valueOf(splits[0]) == 1) {
      click = 1.0;
      nonclick = 0.0;
    } else {
      click = 0.0;
      nonclick = 1.0;
    }

    for (int i = 1; i < splits.length; i++) {
      int index = Integer.parseInt(splits[i]);
      x.set(index, 1.0);
    }

    LabeledData labeledData = new LabeledData(x, nonclick);
    labeledData.setY1(click);
    return labeledData;
  }
}
