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
 *
 */

package com.tencent.angel.ml.sketchML;

import com.tencent.angel.ml.feature.LabeledData;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.math.vector.TDoubleVector;
import com.tencent.angel.ml.optimizer.sgd.L2LogLoss;
import com.tencent.angel.ml.optimizer.sgd.Loss;
import com.tencent.angel.ml.utils.DataParser;
import com.tencent.angel.utils.Sort;
import it.unimi.dsi.fastutil.doubles.DoubleComparator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SketchMLTest {
  private static final Log LOG = LogFactory.getLog(SketchMLTest.class);
  public List<LabeledData> loadData(String dataPath, int maxDim) throws IOException {
    List<LabeledData> ret = new ArrayList<LabeledData>();
    File dir = new File(dataPath);
    List<String> files = new ArrayList<String>();
    if (dir.isFile()) {
      files.add(dataPath);
    } else if (dir.isDirectory()) {
      List<String> fileList = Arrays.asList(dir.list());
      for (String fileName : fileList) {
        if (fileName.contains(".pkl"))
          continue;
        fileName = dataPath + "\\" + fileName;
        files.add(fileName);
      }
    } else {
      return ret;
    }
    for (String fileName : files) {
      File curFile = new File(fileName);
      if (curFile.isFile() && curFile.exists()) {
        BufferedReader reader =
          new BufferedReader(new InputStreamReader(new FileInputStream(curFile)));
        String line;
        while (null != (line = reader.readLine())) {
          LabeledData ins = DataParser.parseVector(line, maxDim, "libsvm", true);
          ret.add(ins);
        }
      }
      LOG.info(fileName);
    }
    return ret;
  }

  public void trainGD(List<LabeledData> dataset, int modelDim, int maxEpoch, double reg, double lr) {
    DenseDoubleVector w = new DenseDoubleVector(modelDim);
    Loss logLoss = new L2LogLoss(reg);

    for (int epoch = 0; epoch < maxEpoch; epoch++) {
      DenseDoubleVector grad = new DenseDoubleVector(modelDim);
      double loss = 0;
      for (LabeledData ins : dataset) {
        double pre = w.dot(ins.getX());
        double gradScalar = logLoss.grad(pre, ins.getY());
        grad.plusBy(ins.getX(), -1.0 * gradScalar);
        loss += logLoss.loss(pre, ins.getY());
      }
      grad.timesBy(1.0 / dataset.size());

      if (logLoss.isL2Reg()) {
        for (int dim = 0; dim < grad.getDimension(); dim++) {
          if (grad.get(dim) > 10e-7) {
            grad.set(dim, grad.get(dim) + w.get(dim) * (logLoss.getRegParam()));
          }
        }
      }
      w.plusBy(grad, -1.0 * lr);
      loss += logLoss.getReg(w);
      LOG.info(String.format("Epoch[%d], loss[%f]", epoch, loss));
    }

  }

  public void trainSGD(List<LabeledData> dataset, int modelDim,
                       int maxEpoch, double reg, double lr, int batchSize) throws IOException, InterruptedException {
    DenseDoubleVector w = new DenseDoubleVector(modelDim);
    Loss logLoss = new L2LogLoss(reg);
    int batchNum = dataset.size() / batchSize;
    LOG.info("Batch number: " + batchNum);

    for (int epoch = 0; epoch < maxEpoch; epoch++) {

      int insIdx = 0;

      for (int batch = 0; batch < batchNum; batch++) {
        DenseDoubleVector grad = new DenseDoubleVector(modelDim);
        double loss = 0;
        for (int i = insIdx; i < insIdx + batchSize; i++) {
          LabeledData ins = dataset.get(i);
          double pre = w.dot(ins.getX());
          double gradScalar = logLoss.grad(pre, ins.getY());
          grad.plusBy(ins.getX(), -1.0 * gradScalar);
          loss += logLoss.loss(pre, ins.getY());
        }
        insIdx += batchSize;
        grad.timesBy(1.0 / batchSize);

        if (logLoss.isL2Reg()) {
          for (int dim = 0; dim < grad.getDimension(); dim++) {
            if (grad.get(dim) > 10e-7) {
              grad.set(dim, grad.get(dim) + w.get(dim) * (logLoss.getRegParam()));
            }
          }
        }

        //grad.timesBy(-1.0 * lr);

        AvgQSketch qSketch = new AvgQSketch(1000);
        qSketch.create(grad);

        CMSketch cmSketch = new CMSketch(1000);
        for (int i = 0; i < grad.getDimension(); i++) {
          cmSketch.insert(i, qSketch.indexOf(grad.get(i)));
        }

        int[] distArr = new int[1000];

        for (int i = 0; i < grad.getDimension(); i++) {
          int trueFreq = qSketch.indexOf(grad.get(i));
          int cmFreq = cmSketch.get(i);
          int dist = Math.abs(trueFreq - cmFreq);
          if (dist >= distArr.length) {
            LOG.info(trueFreq + " : " + cmFreq);
          }
          distArr[dist] += 1;
          grad.set(i, qSketch.get(cmFreq));
        }

        //System.out.println(Arrays.toString(distArr));

        w.plusBy(grad, -1.0 * lr);
        loss += logLoss.getReg(w);
        //System.out.println(String.format("Epoch[%d] batch[%d], loss[%f]", epoch, batch, loss));
      }

      eval(dataset, w, logLoss);

    }

  }

  public void eval(List<LabeledData> dataBlock, TDoubleVector weight, Loss lossFunc) throws IOException, InterruptedException {

    int totalNum = dataBlock.size();
    //System.out.println("Start calculate loss and auc, sample number: " + totalNum);

    long startTime = System.currentTimeMillis();
    double loss = 0.0;

    double[] scoresArray = new double[totalNum];
    double[] labelsArray = new double[totalNum];
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

      scoresArray[i] = pre;
      labelsArray[i] = data.getY();

      loss += lossFunc.loss(pre, data.getY());
    }

    loss += lossFunc.getReg(weight);

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

    //System.out.println("Sort cost " + (System.currentTimeMillis() - sortStartTime) + "ms, Scores list size: "
    //        + scoresArray.length + ", sorted values:" + scoresArray[0] + ","
    //        + scoresArray[scoresArray.length / 5] + "," + scoresArray[scoresArray.length / 3] + ","
    //        + scoresArray[scoresArray.length / 2] + "," + scoresArray[scoresArray.length - 1]);

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
    //System.out.println("M = " + M + ", N = " + N + ", sigma = " + sigma + ", AUC = " + aucResult);

    double precision = (double) (truePos + trueNeg) / totalNum;
    double trueRecall = (double) truePos / (truePos + falseNeg);
    double falseRecall = (double) trueNeg / (trueNeg + falsePos);


    LOG.info(String.format(
            "validate cost %d ms, loss= %.5f, auc=%.5f, "
                    + "precision=%.5f, trueRecall=%.5f, falseRecall=%.5f",
            System.currentTimeMillis() - startTime, loss, aucResult, precision, trueRecall,
            falseRecall));

    //System.out.println(String.format("Validation TP=%d, TN=%d, FP=%d, FN=%d", truePos, trueNeg, falsePos,falseNeg));
  }

  public static void main(String[] argv) throws IOException, InterruptedException {
    SketchMLTest test = new SketchMLTest();
    List<LabeledData> dataset = test.loadData("E:\\file\\liubo-gen-data\\data_file\\N700000_d47000_c2_nnz200_sp0.200000\\0", 47000);
    test.trainSGD(dataset, 47001, 50, 0.01, 0.05, 1000);
  }
}
