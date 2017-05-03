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
import com.tencent.angel.ml.math.vector.SparseDoubleVector;
import com.tencent.angel.ml.math.vector.SparseDummyVector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.Iterator;

public class LBFGS {

  private static final Log LOG = LogFactory.getLog(LBFGS.class);

  public static void train(ADMMState state, int maxIterNum, int lbfgshistory, double rhoADMM,
      double[] z, int iterationADMM) {

    int localFeatureNum = state.localFeatureNum;


    double[] xx = new double[localFeatureNum];
    double[] xNew = new double[localFeatureNum];
    System.arraycopy(state.x, 0, xx, 0, localFeatureNum);
    System.arraycopy(xx, 0, xNew, 0, localFeatureNum);

    double[] g = new double[localFeatureNum];
    double[] gNew = new double[localFeatureNum];

    double[] dir = new double[localFeatureNum];

    ArrayList<double[]> s = new ArrayList<>();
    ArrayList<double[]> y = new ArrayList<>();
    ArrayList<Double> rhoLBFGS = new ArrayList<>();

    int iter = 1;

    double loss = getGradientLoss(state, xx, rhoADMM, g, z);
    System.arraycopy(g, 0, gNew, 0, localFeatureNum);

    while (iter < maxIterNum) {
      twoLoop(s, y, rhoLBFGS, g, localFeatureNum, dir);

      loss = linearSearch(xx, xNew, dir, gNew, loss, iter, state, rhoADMM, z);

      String infoMsg =
          "state feature num=" + state.localFeatureNum + " admm iteration=" + iterationADMM
              + " lbfgs iteration=" + iter + " loss=" + loss;
      LOG.info(infoMsg);

      shift(localFeatureNum, lbfgshistory, xx, xNew, g, gNew, s, y, rhoLBFGS);

      iter++;
    }

    System.arraycopy(xx, 0, state.x, 0, localFeatureNum);

  }

  public static void train(ADMMState state, int maxIterNum, int lbfgshistory, double rhoADMM,
      SparseDoubleVector z, int iterationADMM) {

    int localFeatureNum = state.localFeatureNum;


    double[] xx = new double[localFeatureNum];
    double[] xNew = new double[localFeatureNum];
    System.arraycopy(state.x, 0, xx, 0, localFeatureNum);
    System.arraycopy(xx, 0, xNew, 0, localFeatureNum);

    double[] g = new double[localFeatureNum];
    double[] gNew = new double[localFeatureNum];

    double[] dir = new double[localFeatureNum];

    ArrayList<double[]> s = new ArrayList<>();
    ArrayList<double[]> y = new ArrayList<>();
    ArrayList<Double> rhoLBFGS = new ArrayList<>();

    int iter = 1;

    double loss = getGradientLoss(state, xx, rhoADMM, g, z);
    System.arraycopy(g, 0, gNew, 0, localFeatureNum);

    while (iter < maxIterNum) {
      twoLoop(s, y, rhoLBFGS, g, localFeatureNum, dir);

      loss = linearSearch(xx, xNew, dir, gNew, loss, iter, state, rhoADMM, z);

      String infoMsg =
          "state feature num=" + state.localFeatureNum + " admm iteration=" + iterationADMM
              + " lbfgs iteration=" + iter + " loss=" + loss;
      LOG.info(infoMsg);

      shift(localFeatureNum, lbfgshistory, xx, xNew, g, gNew, s, y, rhoLBFGS);

      iter++;
    }

    System.arraycopy(xx, 0, state.x, 0, localFeatureNum);

  }

  static double getGradientLoss(ADMMState state, double[] localX, double rhoADMM, double[] g,
      double[] z) {
    double loss = 0.0;

    int localFeatureNum = state.localFeatureNum;
    int[] localToGlobal = state.local2Global;

    for (int i = 0; i < localFeatureNum; i++) {
      double temp = localX[i] - z[localToGlobal[i]] + state.u[i];
      g[i] = rhoADMM * temp;
      loss += 0.5 * rhoADMM * temp * temp;
    }

    Iterator<LabeledData> iter = state.instances.iterator();
    while (iter.hasNext()) {
      LabeledData data = iter.next();
      SparseDummyVector sample = (SparseDummyVector) data.getX();
      double neg = data.getY();
      double pos = data.getY1();

      double score = 0.0;
      int[] indices = sample.getIndices();
      for (int i = 0; i < sample.getNonzero(); i++) {
        score += localX[indices[i]];
      }

      double clkProb;
      double clkLoss;

      double nonclkProb;
      double nonclkLoss;

      if (score < -30) {
        clkLoss = -score;
        clkProb = 0;
        nonclkLoss = 0;
        nonclkProb = 1;
      } else if (score > 30) {
        clkLoss = 0;
        clkProb = 1;
        nonclkLoss = score;
        nonclkProb = 0;
      } else {
        double temp = 1.0 + Math.exp(-score);
        clkLoss = Math.log(temp);
        clkProb = 1.0 / temp;
        nonclkLoss = score + clkLoss;
        nonclkProb = 1 - clkProb;
      }
      loss += pos * clkLoss + neg * nonclkLoss;
      double mul = neg * clkProb - pos * nonclkProb;
      for (int i = 0; i < sample.getNonzero(); i++) {
        g[indices[i]] += mul;
      }
    }
    return loss;
  }

  static double getGradientLoss(ADMMState state, double[] localX, double rhoADMM, double[] g,
      SparseDoubleVector z) {
    double loss = 0.0;

    int localFeatureNum = state.localFeatureNum;
    int[] localToGlobal = state.local2Global;

    for (int i = 0; i < localFeatureNum; i++) {
      // double temp = localX[i] - z[localToGlobal[i]] + state.u[i];
      double temp = localX[i] - z.get(localToGlobal[i]) + state.u[i];
      g[i] = rhoADMM * temp;
      loss += 0.5 * rhoADMM * temp * temp;
    }

    Iterator<LabeledData> iter = state.instances.iterator();
    while (iter.hasNext()) {
      LabeledData data = iter.next();
      SparseDummyVector sample = (SparseDummyVector) data.getX();
      double neg = data.getY();
      double pos = data.getY1();

      double score = 0.0;
      int[] indices = sample.getIndices();
      for (int i = 0; i < sample.getNonzero(); i++) {
        score += localX[indices[i]];
      }

      double clkProb;
      double clkLoss;

      double nonclkProb;
      double nonclkLoss;

      if (score < -30) {
        clkLoss = -score;
        clkProb = 0;
        nonclkLoss = 0;
        nonclkProb = 1;
      } else if (score > 30) {
        clkLoss = 0;
        clkProb = 1;
        nonclkLoss = score;
        nonclkProb = 0;
      } else {
        double temp = 1.0 + Math.exp(-score);
        clkLoss = Math.log(temp);
        clkProb = 1.0 / temp;
        nonclkLoss = score + clkLoss;
        nonclkProb = 1 - clkProb;
      }
      loss += pos * clkLoss + neg * nonclkLoss;
      double mul = neg * clkProb - pos * nonclkProb;
      for (int i = 0; i < sample.getNonzero(); i++) {
        g[indices[i]] += mul;
      }
    }
    return loss;
  }

  static double getLoss(ADMMState state, double[] localX, double rhoADMM, double[] z) {
    double loss = 0.0;

    int localFeatureNum = state.localFeatureNum;
    int[] localToGlobal = state.local2Global;

    for (int i = 0; i < localFeatureNum; i++) {
      double temp = localX[i] - z[localToGlobal[i]] + state.u[i];
      loss += 0.5 * rhoADMM * temp * temp;
    }

    Iterator<LabeledData> iter = state.instances.iterator();
    while (iter.hasNext()) {
      LabeledData data = iter.next();
      SparseDummyVector sample = (SparseDummyVector) data.getX();
      double neg = data.getY();
      double pos = data.getY1();

      double score = 0.0;
      int[] indices = sample.getIndices();
      for (int i = 0; i < sample.getNonzero(); i++) {
        score += localX[indices[i]];
      }

      double clkLoss;
      double nonclkLoss;

      if (score < -30) {
        clkLoss = -score;
        nonclkLoss = 0;
      } else if (score > 30) {
        clkLoss = 0;
        nonclkLoss = score;
      } else {
        double temp = 1.0 + Math.exp(-score);
        clkLoss = Math.log(temp);
        nonclkLoss = score + clkLoss;
      }
      loss += pos * clkLoss + neg * nonclkLoss;
    }

    return loss;
  }

  static double getLoss(ADMMState state, double[] localX, double rhoADMM, SparseDoubleVector z) {
    double loss = 0.0;

    int localFeatureNum = state.localFeatureNum;
    int[] localToGlobal = state.local2Global;

    // for (int i = 0; i < localFeatureNum; i ++) {
    // double temp = localX[i] - z[localToGlobal[i]] + state.u[i];
    // loss += 0.5 * rhoADMM * temp * temp;
    // }
    for (int i = 0; i < localFeatureNum; i++) {
      double temp = localX[i] - z.get(localToGlobal[i]) + state.u[i];
      loss += 0.5 * rhoADMM * temp * temp;
    }


    Iterator<LabeledData> iter = state.instances.iterator();
    while (iter.hasNext()) {
      LabeledData data = iter.next();
      SparseDummyVector sample = (SparseDummyVector) data.getX();
      double neg = data.getY();
      double pos = data.getY1();

      double score = 0.0;
      int[] indices = sample.getIndices();
      for (int i = 0; i < sample.getNonzero(); i++) {
        score += localX[indices[i]];
      }

      double clkLoss;
      double nonclkLoss;

      if (score < -30) {
        clkLoss = -score;
        nonclkLoss = 0;
      } else if (score > 30) {
        clkLoss = 0;
        nonclkLoss = score;
      } else {
        double temp = 1.0 + Math.exp(-score);
        clkLoss = Math.log(temp);
        nonclkLoss = score + clkLoss;
      }
      loss += pos * clkLoss + neg * nonclkLoss;
    }

    return loss;
  }

  static void twoLoop(ArrayList<double[]> s, ArrayList<double[]> y, ArrayList<Double> rhoLBFGS,
      double[] g, int localFeatureNum, double[] dir) {

    times(dir, g, -1, localFeatureNum);

    int count = s.size();
    if (count != 0) {
      double[] alphas = new double[count];
      for (int i = count - 1; i >= 0; i--) {
        alphas[i] = -dot(s.get(i), dir, localFeatureNum) / rhoLBFGS.get(i);
        timesBy(dir, y.get(i), alphas[i], localFeatureNum);
      }

      double yDotY = dot(y.get(y.size() - 1), y.get(y.size() - 1), localFeatureNum);
      double scalar = rhoLBFGS.get(rhoLBFGS.size() - 1) / yDotY;
      timesBy(dir, scalar, localFeatureNum);

      for (int i = 0; i < count; i++) {
        double beta = dot(y.get(i), dir, localFeatureNum) / rhoLBFGS.get(i);
        timesBy(dir, s.get(i), -alphas[i] - beta, localFeatureNum);
      }
    }

  }

  static double linearSearch(double[] x, double[] xNew, double[] dir, double[] gNew,
      double oldLoss, int iteration, ADMMState state, double rhoADMM, double[] z) {

    int localFeatureNum = state.localFeatureNum;

    double loss = Double.MAX_VALUE;
    double origDirDeriv = dot(dir, gNew, localFeatureNum);

    // if a non-descent direction is chosen, the line search will break anyway, so throw here
    // The most likely reason for this is a bug in your function's gradient computation
    if (origDirDeriv >= 0) {
      LOG.error(String.format("L-BFGS chose a non-descent direction, check your gradient!"));
      return 0.0;
    }

    double alpha = 1.0;
    double backoff = 0.5;
    if (iteration == 1) {
      alpha = 1 / Math.sqrt(dot(dir, dir, localFeatureNum));
      backoff = 0.1;
    }

    double c1 = 1e-4;
    int i = 1, step = 20;

    while ((loss > oldLoss + c1 * origDirDeriv * alpha) && (step > 0)) {
      timesBy(xNew, x, dir, alpha, localFeatureNum);
      loss = getLoss(state, xNew, rhoADMM, z);
      String infoMsg =
          "state feature num=" + state.localFeatureNum + " lbfgs iteration=" + iteration
              + " line search iteration=" + i + " end loss=" + loss + " alpha=" + alpha
              + " oldloss=" + oldLoss + " delta=" + (c1 * origDirDeriv * alpha);
      LOG.info(infoMsg);
      alpha *= backoff;
      i++;
      step -= 1;
    }

    getGradientLoss(state, xNew, rhoADMM, gNew, z);
    return loss;
  }

  static double linearSearch(double[] x, double[] xNew, double[] dir, double[] gNew,
      double oldLoss, int iteration, ADMMState state, double rhoADMM, SparseDoubleVector z) {

    int localFeatureNum = state.localFeatureNum;

    double loss = Double.MAX_VALUE;
    double origDirDeriv = dot(dir, gNew, localFeatureNum);

    // if a non-descent direction is chosen, the line search will break anyway, so throw here
    // The most likely reason for this is a bug in your function's gradient computation
    if (origDirDeriv >= 0) {
      LOG.error(String.format("L-BFGS chose a non-descent direction, check your gradient!"));
      return 0.0;
    }

    double alpha = 1.0;
    double backoff = 0.5;
    if (iteration == 1) {
      alpha = 1 / Math.sqrt(dot(dir, dir, localFeatureNum));
      backoff = 0.1;
    }

    double c1 = 1e-4;
    int i = 1, step = 20;

    while ((loss > oldLoss + c1 * origDirDeriv * alpha) && (step > 0)) {
      timesBy(xNew, x, dir, alpha, localFeatureNum);
      loss = getLoss(state, xNew, rhoADMM, z);
      String infoMsg =
          "state feature num=" + state.localFeatureNum + " lbfgs iteration=" + iteration
              + " line search iteration=" + i + " end loss=" + loss + " alpha=" + alpha
              + " oldloss=" + oldLoss + " delta=" + (c1 * origDirDeriv * alpha);
      LOG.info(infoMsg);
      alpha *= backoff;
      i++;
      step -= 1;
    }

    getGradientLoss(state, xNew, rhoADMM, gNew, z);
    return loss;
  }

  static void shift(int localFeatureNum, int lbfgsHistory, double[] x, double[] xNew, double[] g,
      double[] gNew, ArrayList<double[]> s, ArrayList<double[]> y, ArrayList<Double> rhoLBFGS) {
    int length = s.size();

    if (length < lbfgsHistory) {
      s.add(new double[localFeatureNum]);
      y.add(new double[localFeatureNum]);
    } else {
      double[] temp = s.remove(0);
      s.add(temp);
      temp = y.remove(0);
      y.add(temp);
      rhoLBFGS.remove(0);
    }

    double[] lastS = s.get(s.size() - 1);
    double[] lastY = y.get(y.size() - 1);

    timesBy(lastS, xNew, x, -1, localFeatureNum);
    timesBy(lastY, gNew, g, -1, localFeatureNum);
    double rho = dot(lastS, lastY, localFeatureNum);
    rhoLBFGS.add(rho);

    System.arraycopy(xNew, 0, x, 0, localFeatureNum);
    System.arraycopy(gNew, 0, g, 0, localFeatureNum);
  }

  static void times(double[] a, double[] b, double x, int length) {
    for (int i = 0; i < length; i++)
      a[i] = b[i] * x;
  }

  static void timesBy(double[] a, double[] b, double x, int length) {
    for (int i = 0; i < length; i++)
      a[i] += b[i] * x;
  }

  static void timesBy(double[] a, double[] b, double[] c, double x, int length) {
    for (int i = 0; i < length; i++)
      a[i] = b[i] + c[i] * x;
  }

  static void timesBy(double[] a, double x, int length) {
    for (int i = 0; i < length; i++)
      a[i] *= x;
  }

  static double dot(double[] a, double[] b, int length) {
    double ret = 0.0;
    for (int i = 0; i < length; i++)
      ret += a[i] * b[i];
    return ret;
  }
}
