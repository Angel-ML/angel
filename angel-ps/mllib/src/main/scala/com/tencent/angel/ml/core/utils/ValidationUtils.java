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


package com.tencent.angel.ml.core.utils;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.core.graphsubmit.GraphPredictResult;
import com.tencent.angel.ml.core.graphsubmit.SoftmaxPredictResult;
import com.tencent.angel.ml.feature.LabeledData;
import com.tencent.angel.ml.model.MLModel;
import com.tencent.angel.ml.core.optimizer.loss.LossFunc;
import com.tencent.angel.ml.predict.PredictResult;
import com.tencent.angel.utils.Sort;
import com.tencent.angel.worker.storage.DataBlock;
import it.unimi.dsi.fastutil.doubles.DoubleComparator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;

import java.util.*;

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

  private DataBlock<?> predicted;
  private double[] labels;
  private int totalNum;

  public ValidationUtils(DataBlock<LabeledData> dataBlock, MLModel model) {
    long startTime = System.currentTimeMillis();
    this.totalNum = dataBlock.size();
    this.predicted = model.predict(dataBlock);
    this.labels = new double[totalNum];

    try {
      for (int i = 0; i < totalNum; i++) {
        labels[i] = dataBlock.loopingRead().getY();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }


    long cost = System.currentTimeMillis() - startTime;


    LOG.debug(String.format("validate samples is %s, and the cost is %d ms", totalNum, cost));
  }

  /**
   * validate loss and precision
   *
   * @param lossFunc: the lossFunc used for prediction
   */
  public double calLossPrecision(LossFunc lossFunc) throws IOException, InterruptedException {
    long startTime = System.currentTimeMillis();

    double loss = 0.0;
    int truePos = 0; // ground truth: positive, precision: positive
    int falsePos = 0; // ground truth: negative, precision: positive
    int trueNeg = 0; // ground truth: negative, precision: negative
    int falseNeg = 0; // ground truth: positive, precision: negative

    for (int i = 0; i < totalNum; i++) {
      PredictResult predRes = (PredictResult) predicted.get(i);
      if (predRes.pred() * labels[i] >= 0) {
        if (predRes.pred() > 0) {
          truePos++;
        } else {
          trueNeg++;
        }
      } else {
        if (predRes.pred() > 0) {
          falsePos++;
        } else {
          falseNeg++;
        }
      }
      loss += lossFunc.loss(predRes.pred(), labels[i]);
    }

    long cost = System.currentTimeMillis() - startTime;

    double precision = (double) (truePos + trueNeg) / totalNum;
    double trueRecall = (double) truePos / (truePos + falseNeg);
    double falseRecall = (double) trueNeg / (trueNeg + falsePos);

    LOG.debug(String
      .format("validate cost %d ms, loss= %.5f, precision=%.5f, trueRecall=%.5f, falseRecall=%.5f",
        cost, loss, precision, trueRecall, falseRecall));

    LOG.debug(
      String.format("Validation TP=%d, TN=%d, FP=%d, FN=%d", truePos, trueNeg, falsePos, falseNeg));

    return loss;
  }

  /**
   * metrics for multi-classification, calculate the accuracy  = trueCount / totalNum
   *
   * @param lossFunc
   * @return loss, accuracy
   * @throws IOException
   * @throws InterruptedException
   */
  public Tuple2<Double, Double> calMulMetrics(LossFunc lossFunc)
    throws IOException, InterruptedException {

    long startTime = System.currentTimeMillis();

    double loss = 0.0;
    // ground truth: positive, precision: positive
    Map<Double, Double> truePos = new HashMap<Double, Double>();

    for (int i = 0; i < totalNum; i++) {
      PredictResult predRes = (PredictResult) predicted.get(i);

      if (predRes.label() == labels[i]) {
        double count = truePos.getOrDefault(labels[i], 0.0);
        truePos.put(labels[i], count + 1.0);
      }

      if (predRes instanceof GraphPredictResult) {
        loss += lossFunc.loss(predRes.proba(), labels[i]);
      } else if (predRes instanceof SoftmaxPredictResult) {
        loss += lossFunc.loss(((SoftmaxPredictResult)predRes).trueProba(), labels[i]);
      } else {
        throw new AngelException("PredictResult Error!");
      }

    }

    long cost = System.currentTimeMillis() - startTime;

    double sum = 0.0;
    for (double count : truePos.values()) {
      sum += count;
    }
    double accuracy = sum / totalNum;

    LOG
      .debug(String.format("validate cost %d ms, loss= %.5f, accuracy=%.5f", cost, loss, accuracy));

    return new Tuple2<>(loss, accuracy);
  }

  /**
   * validate loss, AUC and precision
   *
   * @param lossFunc: the lossFunc used for prediction
   */
  public Tuple5<Double, Double, Double, Double, Double> calMetrics(LossFunc lossFunc)
    throws IOException, InterruptedException {
    LOG.debug("Start calculate loss and auc, sample number: " + totalNum);

    long startTime = System.currentTimeMillis();
    double loss = 0.0;

    double[] scoresArray = new double[totalNum];
    double[] labelsArray = new double[totalNum];
    double truePos = 0; // ground truth: positive, precision: positive
    double falsePos = 0; // ground truth: negative, precision: positive
    double trueNeg = 0; // ground truth: negative, precision: negative
    double falseNeg = 0; // ground truth: positive, precision: negative

    for (int i = 0; i < totalNum; i++) {
      PredictResult predRes = (PredictResult) predicted.get(i);
      if (predRes.pred() * labels[i] >= 0) {
        if (predRes.pred() > 0) {
          truePos++;
        } else {
          trueNeg++;
        }
      } else {
        if (predRes.pred() > 0) {
          falsePos++;
        } else {
          falseNeg++;
        }
      }

      scoresArray[i] = predRes.proba();
      labelsArray[i] = labels[i];

      loss += lossFunc.loss(predRes.pred(), labels[i]);
    }

    double precision = (truePos + trueNeg) / totalNum;

    Tuple3<Double, Double, Double> tuple3 =
      calAUC(scoresArray, labelsArray, truePos, trueNeg, falsePos, falseNeg);
    double aucResult = tuple3._1();
    double trueRecall = tuple3._2();
    double falseRecall = tuple3._3();

    long cost = System.currentTimeMillis() - startTime;

    LOG.debug(String
      .format("validate cost %d ms, loss= %.5f, precision=%.5f, trueRecall=%.5f, falseRecall=%.5f",
        cost, loss, precision, trueRecall, falseRecall));

    return new Tuple5<>(loss, precision, aucResult, trueRecall, falseRecall);
  }

  private Tuple3<Double, Double, Double> calAUC(double[] scoresArray, double[] labelsArray,
    double truePos, double trueNeg, double falsePos, double falseNeg) {
    long startTime = System.currentTimeMillis();

    Sort.quickSort(scoresArray, labelsArray, 0, totalNum, ValidationUtils.cmp);

    LOG.debug("Sort cost " + (System.currentTimeMillis() - startTime) + "ms, Scores list size: "
      + scoresArray.length + ", sorted values:" + scoresArray[0] + "," + scoresArray[
      scoresArray.length / 5] + "," + scoresArray[scoresArray.length / 3] + "," + scoresArray[
      scoresArray.length / 2] + "," + scoresArray[scoresArray.length - 1]);

    long M = 1; // positive sample
    long N = 1; // negtive sample
    for (int i = 0; i < totalNum; i++) {
      if (labelsArray[i] == 1) {
        M++;
      } else {
        N++;
      }
    }
    double sigma = 0;
    for (long i = totalNum-1; i >= 0; i--) {
      if (labelsArray[(int) i] == 1.0) {
        sigma += i + 1;
      }
    }

    double aucResult = (sigma - (M + 1) * M / 2) / M / N;
    LOG.debug("M = " + M + ", N = " + N + ", sigma = " + sigma + ", AUC = " + aucResult);

    double trueRecall = truePos / (truePos + falseNeg);
    double falseRecall = trueNeg / (trueNeg + falsePos);

    LOG.debug(String.format("validate cost %d ms, auc=%.3f, trueRecall=%.3f, falseRecall=%.3f",
      System.currentTimeMillis() - startTime, aucResult, trueRecall, falseRecall));

    LOG.debug(String
      .format("Validation TP=%.0f, TN=%.0f, FP=%.0f, FN=%.0f", truePos, trueNeg, falsePos,
        falseNeg));

    return new Tuple3<>(aucResult, trueRecall, falseRecall);
  }


  /**
   * Calculate MSE, RMSE, MAE and R2
   *
   * @throws IOException
   * @throws InterruptedException
   */
  public Tuple4<Double, Double, Double, Double> calMSER2()
    throws IOException, InterruptedException {
    long startTime = System.currentTimeMillis();

    double uLoss = 0.0;// the regression sum of squares
    double maeLossSum = 0.0;
    double trueSum2 = 0.0;
    double trueSum = 0.0;// the sum of true y
    for (int i = 0; i < totalNum; i++) {
      PredictResult predRes = (PredictResult) predicted.get(i);
      uLoss += Math.pow(predRes.pred() - labels[i], 2);
      maeLossSum += Math.abs(predRes.pred() - labels[i]);
      trueSum2 += Math.pow(labels[i], 2);
      trueSum += labels[i];
    }

    double MSE = uLoss / totalNum;
    double RMSE = Math.sqrt(MSE);
    double MAE = maeLossSum / totalNum;
    double trueAvg = trueSum / totalNum;
    double R2 = 1 - uLoss / (trueSum2 - totalNum * trueAvg * trueAvg);

    LOG.info(String
      .format("validate %d samples cost %d ms, MSE= %.5f ,RMSE= %.5f ,MAE=%.5f ," + "R2= %.5f",
        totalNum, System.currentTimeMillis() - startTime, MSE, RMSE, MAE, R2));

    return new Tuple4<>(MSE, RMSE, MAE, R2);
  }
}
