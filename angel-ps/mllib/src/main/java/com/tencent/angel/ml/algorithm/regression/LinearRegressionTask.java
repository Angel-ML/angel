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

package com.tencent.angel.ml.algorithm.regression;

import java.io.IOException;

import com.tencent.angel.ml.feature.LabeledData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.ml.math.vector.SparseDummyVector;
import com.tencent.angel.worker.task.TrainTask;
import com.tencent.angel.worker.task.TaskContext;

public class LinearRegressionTask extends TrainTask<LongWritable, Text> {
  private final int dimension;

  public LinearRegressionTask(TaskContext taskContext) throws IOException {
    super(taskContext);
    dimension =
        taskContext.getConf().getInt(AngelConfiguration.ANGEL_LINEARREGRESSION_SGD_MAX_DIM,
            AngelConfiguration.DEFAULT_ANGEL_LINEARREGRESSION_SGD_MAX_DIM);
  }

  private static final Log LOG = LogFactory.getLog(LinearRegressionTask.class);
  private boolean addIntercept;

  // private final boolean isTest = true;

  /**
   * @throws Exception
   */
  @Override
  public void run(TaskContext taskContext) throws Exception {
    LOG.debug("------SparseLRTask starts ruuning------");
    LinearRegressionModel model = new LinearRegressionModel(taskContext.getConf());
    model.train(taskContext, trainDataStorage);
  }

  @Override
  public LabeledData parse(LongWritable key, Text line) {
    if (null == line) {
      return null;
    }

    String[] splits = line.toString().split(",");

    if (splits.length < 1) {
      return null;
    }

    SparseDummyVector x = new SparseDummyVector(dimension, splits.length + 1);

    double y = Double.parseDouble(splits[0]);

    for (int i = 1; i < splits.length; i++) {
      int index = Integer.parseInt(splits[i]);
      x.set(index, 1);
    }

    if (addIntercept) {
      x.set(dimension, 1);
    }

    LabeledData result = new LabeledData(x, y);

    return result;
  }
}
