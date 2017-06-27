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

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SketchMLTest {

  public List<LabeledData> loadData(String dataPath, int maxDim) throws IOException {
    List<LabeledData> ret = new ArrayList<LabeledData>();
    File dir = new File(dataPath);
    List<String> files = new ArrayList<String>();
    if (dir.isFile()) {
      System.out.println(dataPath + " is a file.");
      files.add(dataPath);
    } else if (dir.isDirectory()) {
      System.out.println(dataPath + " is a directory.");
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
      System.out.println(fileName);
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
      System.out.println(String.format("Epoch[%d], loss[%f]", epoch, loss));
    }
  }

  public void trainSGD(List<LabeledData> dataset, int modelDim,
                       int maxEpoch, double reg, double lr, int batchSize) throws IOException, InterruptedException {
    DenseDoubleVector w = new DenseDoubleVector(modelDim);
    Loss logLoss = new L2LogLoss(reg);
    int batchNum = dataset.size() / batchSize;
    System.out.println("Batch number: " + batchNum);

    double[] aucArr = new double[maxEpoch];
    double[] lossArr = new double[maxEpoch];
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

        int nnz = 0;

        if (logLoss.isL2Reg()) {
          for (int dim = 0; dim < grad.getDimension(); dim++) {
            if (Math.abs(grad.get(dim)) > 10e-12) {
              nnz++;
              grad.set(dim, grad.get(dim) + w.get(dim) * (logLoss.getRegParam()));
            }
          }

          //grad.timesBy(-1.0 * lr);

          int qSketchSize = 256;
          int cmSketchSize = nnz / 3;

          AvgQSketch qSketch = new AvgQSketch(qSketchSize);
          qSketch.create(grad);
          //System.out.println("Quantile sketch indices: " + Arrays.toString(qSketch.getValues()));
          //System.out.println("Max: " + qSketch.max() + ", min: " + qSketch.min());
          //System.out.println("Quantile sketch count: " + Arrays.toString(qSketch.getCounts()));

          CMSketch cmSketch = new CMSketch(cmSketchSize);
          cmSketch.setZeroIdx(qSketch.getZeroIndex());
          //System.out.println("Zero index: " + qSketch.getZeroIndex() + ", "
          //        + qSketch.get(qSketch.getZeroIndex()) + ", " + qSketch.get(qSketch.getZeroIndex()-1));

          for (int i = 0; i < grad.getDimension(); i++) {
            if (Math.abs(grad.get(i)) > 10e-12) {
              cmSketch.insert(i, qSketch.indexOf(grad.get(i)));
            }
          }

          //cmSketch.distribution();
          //write2File(cmSketch.getTable(0), "E:\\dropbox\\code\\github\\sketchML\\table0");
          //write2File(cmSketch.getTable(1), "E:\\dropbox\\code\\github\\sketchML\\table1");
          //write2File(cmSketch.getTable(2), "E:\\dropbox\\code\\github\\sketchML\\table2");

          int[] distArr = new int[2 * cmSketchSize];
          int[] ratioArr = new int[2 * cmSketchSize];

          int negCount = 0;
          int largeCount = 0;
          int smallCount = 0;
          int zeroGrad = 0;
          for (int i = 0; i < grad.getDimension(); i++) {
            int trueFreq = qSketch.indexOf(grad.get(i));
            int cmFreq = cmSketch.get(i);
            //System.out.println("true freq: " + trueFreq + ", sketch freq: " + cmFreq);

            int dist = trueFreq - cmFreq;
            int ratio = 0;
            distArr[dist + cmSketchSize] += 1;
            if (Math.abs(grad.get(i)) < 10e-12) {
              zeroGrad++;
              continue;
            }
            if (grad.get(i) * qSketch.get(cmFreq) < 0) {
              negCount++;
              //System.out.println("true grad: " + grad.get(i) + ", sketch grad: " + (- qSketch.get(cmFreq)));
              if (Math.abs(grad.get(i)) > Math.abs(qSketch.get(cmFreq))) {
                smallCount++;
              } else if (Math.abs(grad.get(i)) < Math.abs(qSketch.get(cmFreq))) {
                largeCount++;
              }
              ratio = (int) (-qSketch.get(cmFreq) / grad.get(i));
              //ratioArr[ratio]++;
              grad.set(i, -qSketch.get(cmFreq));
            } else if (Math.abs(grad.get(i)) > Math.abs(qSketch.get(cmFreq))) {
              smallCount++;
              //System.out.println("true grad: " + grad.get(i) + ", sketch grad: " + qSketch.get(cmFreq));
              ratio = (int) (qSketch.get(cmFreq) / grad.get(i));
              ratioArr[ratio]++;
              grad.set(i, qSketch.get(cmFreq));
            } else if (Math.abs(grad.get(i)) < Math.abs(qSketch.get(cmFreq))) {
              largeCount++;
              //System.out.println("true grad: " + grad.get(i) + ", sketch grad: " + qSketch.get(cmFreq));
              ratio = (int) (qSketch.get(cmFreq) / grad.get(i));
              //ratioArr[ratio]++;
              grad.set(i, qSketch.get(cmFreq));
            }
          }
          //System.out.println(Arrays.toString(distArr));
          //System.out.println(Arrays.toString(ratioArr));

          //System.out.println("Nnz grad: " + nnz +", zero grad: " + zeroGrad + ", negative grad: " + negCount + ", larger grad: " + largeCount + ", smaller grad: " + smallCount);
          //write2File(distArr, "E:\\dropbox\\code\\github\\sketchML\\error_hist");

          //for (int i = 0; i < grad.getDimension(); i++) {
          //  System.out.println("true grad: " + grad.get(i) + ", sketch grad: " + qSketch.get(qSketch.indexOf(grad.get(i))));
          //  grad.set(i, qSketch.get(qSketch.indexOf(grad.get(i))));
          //}

          w.plusBy(grad, -1.0 * lr);
          loss += logLoss.getReg(w);
        }

      }

      double[] evalResult = eval(dataset, w, logLoss);
      lossArr[epoch] = evalResult[0];
      aucArr[epoch] = evalResult[1];

    }

    System.out.println("Loss: " + Arrays.toString(lossArr));
    System.out.println("Auc: " + Arrays.toString(aucArr));
  }

  public void trainSGD2(List<LabeledData> dataset, int modelDim,
                       int maxEpoch, double reg, double lr, int batchSize) throws IOException, InterruptedException {
    DenseDoubleVector w = new DenseDoubleVector(modelDim);
    Loss logLoss = new L2LogLoss(reg);
    int batchNum = dataset.size() / batchSize;
    System.out.println("Batch number: " + batchNum);

    double[] aucArr = new double[maxEpoch];
    double[] lossArr = new double[maxEpoch];

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
            if (Math.abs(grad.get(dim)) > 10e-12) {
              grad.set(dim, grad.get(dim) + w.get(dim) * (logLoss.getRegParam()));
            }
          }
        }

        int qSketchSize = 255;

        YahooSketch qSketch = new YahooSketch(qSketchSize);
        qSketch.create(grad);
        double[] splits = qSketch.getSplits();
        int maxCount = qSketch.maxCount();

        int[][] rIndex = new int[qSketchSize][maxCount];
        int[] curIdx = new int[qSketchSize];

        int nnz = 0;
        for (int dim = 0; dim < grad.getDimension(); dim++) {
          if (Math.abs(grad.get(dim)) > 10e-12) {
            nnz++;
            int bin = qSketch.indexOf(grad.get(dim));
            rIndex[bin][curIdx[bin]] = dim;
            curIdx[bin]++;
          }
        }

        //System.out.println("Cur index of rIndex: " + Arrays.toString(curIdx));

        // change to delta store
        int bytes = 0;
        for (int i = 0; i < qSketchSize; i++) {
          //System.out.println("Before compression: " + Arrays.toString(rIndex[i]));
          for (int j = curIdx[i] - 1; j > 0; j--) {
            rIndex[i][j] = rIndex[i][j] - rIndex[i][j-1];
            bytes += needBytes(rIndex[i][j]);
          }
          //System.out.println("After compression: " + Arrays.toString(rIndex[i]));
        }

        //System.out.println("Compressed " + nnz + " item to " + bytes
        //        + " bytes, average bytes per item: " + (double) bytes / qSketch.totalCount()
        //        + ", uncompressed bytes: " + 8 * grad.getDimension());

        //for (int i = 0; i < qSketchSize; i++) {
        //  int tmp = 0;
        //  for (int j = 0; j < curIdx[i]; j++) {
        //    tmp += rIndex[i][j];
        //    grad.set(tmp, qSketch.getSplit(i));
        //  }
        //}

        w.plusBy(grad, -1.0 * lr);
        loss += logLoss.getReg(w);
      }

      double[] evalResult = eval(dataset, w, logLoss);
      lossArr[epoch] = evalResult[0];
      aucArr[epoch] = evalResult[1];

    }

    System.out.println("Loss: " + Arrays.toString(lossArr));
    System.out.println("Auc: " + Arrays.toString(aucArr));
  }

  public int needBytes (int item) {
    if (item < 256) {
      return 1;
    } else if (item < 65536) {
      return 2;
    } else if (item < 16777216) {
      return 3;
    } else
      return 4;
  }

  public void write2File(int[] arr, String fileName) throws IOException {
    File f = new File(fileName);
    BufferedWriter writer = new BufferedWriter(new FileWriter(f));
    for (int i = 0; i < arr.length; i++) {
      writer.write(arr[i] + "\n");
    }
    writer.close();
  }

  public double[] eval(List<LabeledData> dataBlock, TDoubleVector weight, Loss lossFunc) throws IOException, InterruptedException {

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

    double precision = (double) (truePos + trueNeg) / totalNum;
    double trueRecall = (double) truePos / (truePos + falseNeg);
    double falseRecall = (double) trueNeg / (trueNeg + falsePos);

    System.out.println(String.format(
            "validate cost %d ms, loss= %.5f, auc=%.5f, "
                    + "precision=%.5f, trueRecall=%.5f, falseRecall=%.5f",
            System.currentTimeMillis() - startTime, loss, aucResult, precision, trueRecall,
            falseRecall));

    return new double[]{loss, aucResult, precision, trueRecall, falseRecall};
  }

  public static void main(String[] argv) throws IOException, InterruptedException {
    SketchMLTest test = new SketchMLTest();
    List<LabeledData> dataset = test.loadData("E:\\file\\liubo-gen-data\\data_file\\N700000_d47000_c2_nnz200_sp0.200000\\0", 47000);
    test.trainSGD(dataset, 47001, 20, 0.01, 0.1, 100);
    //test.trainSGD2(dataset, 47001, 20, 0.01, 0.01, 100);
  }

}
