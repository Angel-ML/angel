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
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.algorithm.utils;

import com.tencent.angel.ml.algorithm.optimizer.sgd.Loss;
import com.tencent.angel.ml.feature.LabeledData;
import com.tencent.angel.ml.math.vector.TDoubleVector;
import com.tencent.angel.utils.Sort;
import com.tencent.angel.worker.storage.Storage;
import it.unimi.dsi.fastutil.doubles.DoubleComparator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 *  Calculate loss, auc and precision values.
 */

public class ValidationUtils {
  private static final Log LOG = LogFactory.getLog(ValidationUtils.class);

  /**
   *  validate loss and precision
   *  @param storage: validation data storage
   *  @param  weight: the weight vector of features
   *  @param lossFunc: the lossFunc used for prediction
   */
  public static void calLossPrecision(Storage<LabeledData> storage,
                                      TDoubleVector weight, Loss lossFunc) throws IOException,
      InterruptedException {
    storage.resetReadIndex();

    long startTime = System.currentTimeMillis();

    int totalNum = storage.getTotalElemNum();
    double loss = 0.0;
    int truePos = 0; // ground truth: positive, precision: positive
    int falsePos = 0; // ground truth: negative, precision: positive
    int trueNeg = 0; // ground truth: negative, precision: negative
    int falseNeg = 0; // ground truth: positive, precision: negative

    for (int i = 0; i < totalNum; i++) {
      LabeledData data = storage.get(i);
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

    double lossPerSp = loss / totalNum;
    double precision = (double) (truePos + trueNeg) / totalNum;
    double trueRecall = (double) truePos / (truePos + falseNeg);
    double falseRecall = (double) trueNeg / (trueNeg + falsePos);

    LOG.info(String.format("validate %d samples cost %d ms, loss per sample= %.5f, " +
        "precision=%.5f, trueRecall=%.5f, falseRecall=%.5f", totalNum, System.currentTimeMillis
        () - startTime, lossPerSp, precision, trueRecall, falseRecall));

    LOG.debug(String.format("Validation TP=%d, TN=%d, FP=%d, FN=%d", truePos, trueNeg, falsePos,
        falseNeg));

  }

  /**
   *  validate loss, AUC and precision
   *  @param storage: validation data storage
   *  @param  weight: the weight vector of features
   *  @param lossFunc: the lossFunc used for prediction
   */
  public static void calLossAucPrecision(Storage<LabeledData> storage,
                                         TDoubleVector weight, Loss lossFunc) throws
      IOException,
      InterruptedException {
    storage.resetReadIndex();

    int totalNum = storage.getTotalElemNum();
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
      LabeledData data = storage.read();
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

    long sortStartTime = System.currentTimeMillis();
    DoubleComparator cmp = new DoubleComparator() {

      @Override
      public int compare(double i, double i1) {
        if (Math.abs(i - i1) < 10e-12) {
          return 0;
        } else {
          return i - i1 > 10e-12 ? 1 : -1;
        }
      }

      @Override
      public int compare(Double o1, Double o2) {
        if (Math.abs(o1 - o2) < 10e-12) {
          return 0;
        } else {
          return o1 - o2 > 10e-12 ? 1 : -1;
        }
      }
    };

    Sort.quickSort(scoresArray, labelsArray, 0, scoresArray.length, cmp);

    LOG.debug("Sort cost " + (System.currentTimeMillis() - sortStartTime) + "ms, Scores list size: "
        + scoresArray.length + ", sorted values:" + scoresArray[0] + ","
        + scoresArray[scoresArray.length / 5] + "," + scoresArray[scoresArray.length / 3] + ","
        + scoresArray[scoresArray.length / 2] + "," + scoresArray[scoresArray.length - 1]);

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

    double lossPerSp = loss / totalNum;
    double precision = (double) (truePos + trueNeg) / totalNum;
    double trueRecall = (double) truePos / (truePos + falseNeg);
    double falseRecall = (double) trueNeg / (trueNeg + falsePos);


    LOG.info(String.format("validate %d samples cost %d ms, loss per sample= %.5f, auc=%.5f, " +
        "precision=%.5f, trueRecall=%.5f, falseRecall=%.5f", totalNum, System.currentTimeMillis
        () - startTime, lossPerSp, aucResult, precision, trueRecall, falseRecall));

    LOG.debug(String.format("Validation TP=%d, TN=%d, FP=%d, FN=%d", truePos, trueNeg, falsePos,
        falseNeg));

  }
}
