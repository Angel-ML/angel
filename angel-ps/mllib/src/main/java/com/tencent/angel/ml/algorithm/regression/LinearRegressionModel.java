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

import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.ml.algorithm.optimizer.sgd.GradientDescent;
import com.tencent.angel.ml.algorithm.optimizer.sgd.SquareLoss;
import com.tencent.angel.ml.feature.LabeledData;
import com.tencent.angel.ml.math.TAbstractVector;
import com.tencent.angel.ml.math.vector.TDoubleVector;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.worker.storage.Storage;
import com.tencent.angel.worker.task.TaskContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.TreeMap;

public class LinearRegressionModel {
  private static final Log LOG = LogFactory.getLog(LinearRegressionModel.class);
  private final int maxIteration;
  private final double lr_0;
  private final double lamda;
  private final int batchSize;
  private final double validateRatio;
  private final double decay;
  private final boolean addIntercept;
  private final SquareLoss squareLoss;
  private final String parameterName;

  public LinearRegressionModel(Configuration conf) {
    this.maxIteration =
        conf.getInt(AngelConfiguration.ANGEL_TASK_ITERATION_NUMBER,
            AngelConfiguration.DEFAULT_ANGEL_TASK_ITERATION_NUMBER);

    this.lr_0 =
        conf.getDouble(AngelConfiguration.ANGEL_LINEARREGRESSION_SGD_LEARNINGRATE,
            AngelConfiguration.DEFAULT_ANGEL_LINEARREGRESSION_SGD_LEARNINGRATE);

    this.lamda =
        conf.getDouble(AngelConfiguration.ANGEL_LINEARREGRESSION_SGD_REGULARIZATION,
            AngelConfiguration.DEFAULT_ANGEL_LINEARREGRESSION_SGD_REGULARIZATION);

    this.batchSize =
        conf.getInt(AngelConfiguration.ANGEL_LINEARREGRESSION_SGD_BATCHSIZE,
            AngelConfiguration.DEFAULT_ANGEL_LINEARREGRESSION_SGD_BATCHSIZE);

    this.validateRatio =
        conf.getDouble(AngelConfiguration.ANGEL_LINEARREGRESSION_SGD_VALIDATERATIO,
            AngelConfiguration.DEFAULT_ANGEL_LINEARREGRESSION_SGD_VALIDATERATIO);

    this.decay =
        conf.getDouble(AngelConfiguration.ANGEL_LINEARREGRESSION_SGD_DECAY,
            AngelConfiguration.DEFAULT_ANGEL_LINEARREGRESSION_SGD_DECAY);

    this.addIntercept =
        conf.getBoolean(AngelConfiguration.ANGEL_LINEARREGRESSION_SGD_INTERCEPT,
            AngelConfiguration.DEFAULT_ANGEL_LINEARREGRESSION_SGD_INTERCEPT);

    this.parameterName =
        conf.get(AngelConfiguration.ANGEL_LINEARREGRESSION_PARAMETERNAME,
            AngelConfiguration.DEFAULT_ANGEL_LINEARREGRESSION_PARAMETERNAME);

    this.squareLoss = new SquareLoss();
  }

  public void train(TaskContext taskContext, Storage<LabeledData> trainDataStorage)
      throws Exception {
    int totalSampleNum = trainDataStorage.getTotalElemNum();
    TDoubleVector weightVector = null;
    long totalTrainStartTime = System.currentTimeMillis();

    MatrixClient matrixClient = taskContext.getMatrix(parameterName);
    int matrixId = matrixClient.getMatrixId();
    int trainSampleNum = (int) (totalSampleNum * (1 - validateRatio));
    while (taskContext.getIteration() < this.maxIteration) {
      long curIterStartTime = System.currentTimeMillis();
      try {
        weightVector = (TDoubleVector) matrixClient.getRow(0);
      } catch (Exception e) {
        LOG.error(
            String.format("Task[%d] fails when get weight vector from PS", taskContext.getTaskId()),
            e);
        return;
      }

      long startTrainTime = System.currentTimeMillis();

      LOG.info(String
          .format(
              "Task[%d] starts training with SGD in clock[%d], get parameter cost %d ms, length is %d , first three weight = [%f] [%f] [%f], sparsity: %f",
              taskContext.getTaskId(), taskContext.getMatrixClock(matrixId), startTrainTime - curIterStartTime,
              weightVector.size(), weightVector.get(0), weightVector.get(1), weightVector.get(0),
              weightVector.sparsity()));


      double lr = lr_0 / Math.sqrt(1.0 + this.decay * taskContext.getIteration());
      GradientDescent.runMiniL2BatchSGD(matrixClient, trainDataStorage, weightVector, lr,
          trainSampleNum, batchSize, squareLoss, lamda);
      validate(trainDataStorage, weightVector);
      matrixClient.clock().get();
      taskContext.increaseIteration();
    }

    long endTime = System.currentTimeMillis();
    LOG.info(String.format("SparseLRTask finishes in %d ms", endTime - totalTrainStartTime));
  }

  private void validate(Storage<LabeledData> featureStorage, TDoubleVector weightVector)
      throws ClassNotFoundException, IOException, InterruptedException {
    int totalCheckNum = 0;
    TreeMap<Integer, Integer> predictMap = new TreeMap<Integer, Integer>();
    double squreSum = 0.0;

    while (true) {
      LabeledData data = featureStorage.read();
      if (data == null) {
        break;
      }
      totalCheckNum++;
      double predictAge = squareLoss.predict(weightVector, data.getX());
      int error = (int) Math.abs((data.getY() - predictAge));
      squreSum += error * error;
      if (predictMap.containsKey(error)) {
        predictMap.put(error, predictMap.get(error) + 1);
      } else {
        predictMap.put(error, 1);
      }
    }

    LOG.info("totalCheckNum = " + totalCheckNum + ", MSE = " + Math.sqrt(squreSum / totalCheckNum));
    for (java.util.Map.Entry<Integer, Integer> entry : predictMap.entrySet()) {
      LOG.info("predict bias = " + entry.getKey() + ", number = " + entry.getValue()
          + ", percent = " + (double) (entry.getValue()) / totalCheckNum);
    }
  }

  @SuppressWarnings("unused")
  private double lossSum(TAbstractVector[] xList, double[] yList, TDoubleVector w, int batchSize) {
    double lossSum = 0;
    for (int i = 0; i < batchSize; i++) {
      lossSum += squareLoss.loss(w.dot(xList[i]), yList[i]);
    }
    return lossSum;
  }
}
