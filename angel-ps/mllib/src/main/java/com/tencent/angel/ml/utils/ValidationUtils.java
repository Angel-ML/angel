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
 */

package com.tencent.angel.ml.utils;

import com.tencent.angel.ml.feature.LabeledData;
import com.tencent.angel.ml.math.vector.TDoubleVector;
import com.tencent.angel.ml.optimizer.sgd.loss.Loss;
import com.tencent.angel.utils.Sort;
import com.tencent.angel.worker.storage.DataBlock;
import it.unimi.dsi.fastutil.doubles.DoubleComparator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

import java.io.IOException;

/**
 * Calculate loss, auc and precision values.
 */

public class ValidationUtils {
  private static final Log LOG = LogFactory.getLog(ValidationUtils.class);

  private static DoubleComparator cmp = new DoubleComparator() {
    @Override public int compare(double i, double i1) {
      if (Math.abs(i - i1) < 10e-12) {
        return 0;
      } else {
        return i - i1 > 10e-12 ? 1 : -1;
      }
    }

    @Override public int compare(Double o1, Double o2) {
      if (Math.abs(o1 - o2) < 10e-12) {
        return 0;
      } else {
        return o1 - o2 > 10e-12 ? 1 : -1;
      }
    }
  };

  /**
   * validate loss and precision
   *
   * @param dataBlock: validation data taskDataBlock
   * @param weight:    the weight vector of features
   * @param lossFunc:  the lossFunc used for prediction
   */
  public static double calLossPrecision(DataBlock<LabeledData> dataBlock, TDoubleVector weight,
    Loss lossFunc) throws IOException, InterruptedException {
    dataBlock.resetReadIndex();

    long startTime = System.currentTimeMillis();

    int totalNum = dataBlock.size();
    double loss = 0.0;
    int truePos = 0; // ground truth: positive, precision: positive
    int falsePos = 0; // ground truth: negative, precision: positive
    int trueNeg = 0; // ground truth: negative, precision: negative
    int falseNeg = 0; // ground truth: positive, precision: negative

    for (int i = 0; i < totalNum; i++) {
      LabeledData data = dataBlock.get(i);
      double pre = lossFunc.predict(weight, data.getX());
      if (pre * data.getY() > 0) {
        if (pre > 0) {
          truePos++;
        } else {
          trueNeg++;
        }
      } else if (pre * data.getY() < 0) {
        if (pre > 0) {
          falsePos++;
        } else {
          falseNeg++;
        }
      }
      loss += lossFunc.loss(pre, data.getY());
    }

    long cost = System.currentTimeMillis() - startTime;

    double precision = (double) (truePos + trueNeg) / totalNum;
    double trueRecall = (double) truePos / (truePos + falseNeg);
    double falseRecall = (double) trueNeg / (trueNeg + falsePos);

    LOG.debug(String.format(
      "validate cost %d ms, loss= %.5f, precision=%.5f, trueRecall=%.5f, " + "falseRecall=%.5f",
      totalNum, cost, loss, precision, trueRecall, falseRecall));

    LOG.debug(
      String.format("Validation TP=%d, TN=%d, FP=%d, FN=%d", truePos, trueNeg, falsePos, falseNeg));

    return loss;
  }

  /**
   * validate loss, AUC and precision
   *
   * @param dataBlock: validation data taskDataBlock
   * @param weight:    the weight vector of features
   * @param lossFunc:  the lossFunc used for prediction
   */
  public static Tuple5<Double, Double, Double, Double, Double> calMetrics(
    DataBlock<LabeledData> dataBlock, TDoubleVector weight, Loss lossFunc)
    throws IOException, InterruptedException {

    dataBlock.resetReadIndex();

    int totalNum = dataBlock.size();
    LOG.debug("Start calculate loss and auc, sample number: " + totalNum);

    long startTime = System.currentTimeMillis();
    double loss = 0.0;

    double[] scoresArray = new double[totalNum];
    double[] labelsArray = new double[totalNum];
    int truePos = 0; // ground truth: positive, precision: positive
    int falsePos = 0; // ground truth: negative, precision: positive
    int trueNeg = 0; // ground truth: negative, precision: negative
    int falseNeg = 0; // ground truth: positive, precision: negative

    for (int i = 0; i < totalNum; i++) {
      LabeledData data = dataBlock.read();

      double pre = lossFunc.predict(weight, data.getX());

      if (pre * data.getY() > 0) {
        if (pre > 0) {
          truePos++;
        } else {
          trueNeg++;
        }
      } else if (pre * data.getY() < 0) {
        if (pre > 0) {
          falsePos++;
        } else {
          falseNeg++;
        }
      }

      scoresArray[i] = pre;
      labelsArray[i] = data.getY();

      loss += lossFunc.loss(pre, data.getY());
    }

    loss += lossFunc.getReg(weight);
    double precision = (double) (truePos + trueNeg) / totalNum;

    Tuple3<Double, Double, Double> tuple3 =
      calAUC(scoresArray, labelsArray, truePos, trueNeg, falsePos, falseNeg);
    double aucResult = tuple3._1();
    double trueRecall = tuple3._2();
    double falseRecall = tuple3._3();

    return new Tuple5(loss, precision, aucResult, trueRecall, falseRecall);
  }

  public static Tuple3<Double, Double, Double> calAUC(double[] scoresArray, double[] labelsArray,
    int truePos, int trueNeg, int falsePos, int falseNeg) {
    long startTime = System.currentTimeMillis();

    Sort.quickSort(scoresArray, labelsArray, 0, scoresArray.length, ValidationUtils.cmp);

    LOG.debug("Sort cost " + (System.currentTimeMillis() - startTime) + "ms, Scores list size: "
      + scoresArray.length + ", sorted values:" + scoresArray[0] + "," + scoresArray[
      scoresArray.length / 5] + "," + scoresArray[scoresArray.length / 3] + "," + scoresArray[
      scoresArray.length / 2] + "," + scoresArray[scoresArray.length - 1]);

    long M = 0; // positive sample
    long N = 0; // negtive sample
    for (int i = 0; i < scoresArray.length; i++) {
      if (labelsArray[i] == 1) {
        M++;
      } else {
        N++;
      }
    }
    double sigma = 0;
    for (long i = M + N - 1; i >= 0; i--) {
      if (labelsArray[(int) i] == 1.0) {
        sigma += i;
      }
    }

    double aucResult = (sigma - (M + 1) * M / 2) / M / N;
    LOG.debug("M = " + M + ", N = " + N + ", sigma = " + sigma + ", AUC = " + aucResult);

    double totalNum = scoresArray.length;
    double trueRecall = (double) truePos / (truePos + falseNeg);
    double falseRecall = (double) trueNeg / (trueNeg + falsePos);

    LOG.debug(String.format("validate cost %d ms, auc=%.5f, trueRecall=%.5f, falseRecall=%.5f",
      System.currentTimeMillis() - startTime, aucResult, trueRecall, falseRecall));

    LOG.debug(
      String.format("Validation TP=%d, TN=%d, FP=%d, FN=%d", truePos, trueNeg, falsePos, falseNeg));

    return new Tuple3<>(aucResult, trueRecall, falseRecall);
  }


  /**
   * Calculate MSE, RMSE, MAE and R2
   *
   * @param dataBlock
   * @param weight
   * @param lossFunc
   * @throws IOException
   * @throws InterruptedException
   */
  public static Tuple4<Double, Double, Double, Double> calMSER2(DataBlock<LabeledData> dataBlock,
    TDoubleVector weight, Loss lossFunc) throws IOException, InterruptedException {
    dataBlock.resetReadIndex();

    long startTime = System.currentTimeMillis();

    int totalNum = dataBlock.size();
    double uLoss = 0.0;// the regression sum of squares
    double vLoss = 0.0; // the residual sum of squares
    double trueSum = 0.0;// the sum of true y
    double maeLossSum = 0.0;
    for (int i = 0; i < totalNum; i++) {
      LabeledData data = dataBlock.get(i);
      double pre = lossFunc.predict(weight, data.getX());
      uLoss += Math.pow(lossFunc.loss(pre, data.getY()), 2);
      trueSum += data.getY();
      maeLossSum += Math.abs(data.getY() - pre);
    }

    double trueAvg = trueSum / totalNum;
    for (int i = 0; i < totalNum; i++) {
      LabeledData data = dataBlock.get(i);
      vLoss += Math.pow(lossFunc.loss(trueAvg, data.getY()), 2);
    }

    double MSE = uLoss / totalNum;
    double RMSE = Math.sqrt(MSE);
    double MAE = maeLossSum / totalNum;
    double R2 = 1 - uLoss / vLoss;

    LOG.info(String
      .format("validate %d samples cost %d ms, MSE= %.5f ,RMSE= %.5f ,MAE=%.5f ," + "R2= %.5f",
        totalNum, System.currentTimeMillis() - startTime, MSE, RMSE, MAE, R2));

    return new Tuple4<>(MSE, RMSE, MAE, R2);
  }
}
