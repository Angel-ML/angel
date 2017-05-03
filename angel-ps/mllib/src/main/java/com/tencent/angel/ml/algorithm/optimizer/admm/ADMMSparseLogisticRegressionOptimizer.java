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
import com.tencent.angel.ml.math.vector.*;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.worker.task.TaskContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class ADMMSparseLogisticRegressionOptimizer {

  private static final Log LOG = LogFactory.getLog(ADMMSparseLogisticRegressionOptimizer.class);

  // private double[] z;
  private SparseDoubleVector z;

  private int feaNum;
  private double t;

  private TaskContext context;

  private final MatrixClient uClient;
  private final MatrixClient wClient;
  private final MatrixClient tClient;
  private final MatrixClient zClient;
  private final MatrixClient lossClient;

  public ADMMSparseLogisticRegressionOptimizer(int feaNum, TaskContext context) throws Exception {
    // z = new double[feaNum];
    z = new SparseDoubleVector(feaNum);
    this.feaNum = feaNum;
    this.context = context;
    uClient = context.getMatrix(ADMMSubmitter.AUC);
    wClient = context.getMatrix(ADMMSubmitter.W);
    tClient = context.getMatrix(ADMMSubmitter.T);
    zClient = context.getMatrix(ADMMSubmitter.Z);
    lossClient = context.getMatrix(ADMMSubmitter.Loss);
  }

  public void train(double lambdaL1, double rho, int N, int length, int maxIterNum,
      int lbfgsNumIteration, ADMMState state) throws Exception {
    train(lambdaL1, rho, N, length, maxIterNum, lbfgsNumIteration, 1E-4, 1E-4, 10, state);
  }

  @SuppressWarnings("rawtypes")
  public void train(double lambdaL1, double rho, int N, int length, int maxIterNum,
      int lbfgsNumIteration, double epsilonConv, double epsilonFeas, int lbfgsHistory,
      ADMMState state) throws Exception {

    for (int iter = 1; iter <= maxIterNum; iter++) {
      LOG.info("[Train] ADMM iteration=" + iter + " start");

      oneRoundComputation(lambdaL1, rho, lbfgsNumIteration, lbfgsHistory, N, iter, state);

      double globalLoss = getGlobalLoss(state, lambdaL1, this.z);
      LOG.info("[Train] ADMM iteration=" + iter + " end loss=" + globalLoss);

      PredictScore.predict(state, z);

      double auc = AucData.calcAuc(state, context);

      LOG.info("[Train] ADMM iteration=" + iter + " train set auc=" + auc);

      tClient.clock().get();
      tClient.getRow(0);

      clockAllMatrix();
      if (context.getTaskIndex() == 0) {
        tClient.zero();
        uClient.zero();
        wClient.zero();
        zClient.zero();
        lossClient.zero();
      }
      clockAllMatrix();

      tClient.clock().get();
      tClient.getRow(0);
    }

    // TDoubleVector z = new DenseDoubleVector(feaNum, this.z);
    // zUpdate(z);
    zUpdate(this.z);
  }

  @SuppressWarnings("rawtypes")
  private void clockAllMatrix() throws Exception {
    List<Future> futureList = new ArrayList<Future>(5);
    futureList.add(tClient.clock());
    futureList.add(uClient.clock());
    futureList.add(wClient.clock());
    futureList.add(zClient.clock());
    futureList.add(lossClient.clock());
    for (int i = 0; i < 5; i++) {
      futureList.get(i).get();
    }
  }

  public void check() throws Exception {
    TDoubleVector w = (TDoubleVector) wClient.getRow(0);
    for (int i = 0; i < feaNum; i++) {
      if (w.get(i) != 0.0) {
        LOG.error("Some error happen w[" + i + "]=" + w.get(i) + " expected value is 0");
      }
    }

    TDoubleVector loss = (TDoubleVector) lossClient.getRow(0);
    if (loss.get(0) != 0.0) {
      LOG.error("Some error happend loss=" + loss.get(0) + " expected value is 0");
    }

    DenseIntVector auc = (DenseIntVector) uClient.getRow(0);
    for (int i = 0; i < auc.size(); i++) {
      if (auc.get(i) != 0) {
        LOG.error("Some error happen auc[" + i + "]=" + auc.get(i) + " expected value is 0");
      }
    }

    TDoubleVector t = (TDoubleVector) tClient.getRow(0);
    if (t.get(0) != 0.0) {
      LOG.error("Some error happend loss=" + t.get(0) + " expected value is 0");
    }

  }

  public void oneRoundComputation(double lambdaL1, double rho, int lbfgsNumIteration,
      int lbfgsHistory, int N, int iter, ADMMState state) throws Exception {
    uUpdate(lbfgsNumIteration, lbfgsHistory, rho, z, iter, state);
    wUpdate(iter, state);
    TDoubleVector z = getZ(state, lambdaL1, rho, N);
    tUpdate(z, iter, state);
  }

  public void uUpdate(int lbfgsNumIteration, int lbfgsHistory, double rho, double[] z, int iter,
      ADMMState state) {

    // u_i = u_i + x_i - z_i
    for (int i = 0; i < state.localFeatureNum; i++) {
      state.u[i] = state.u[i] + state.x[i] - z[state.local2Global[i]];
    }

    // x update
    double loss = LBFGS.getLoss(state, state.x, rho, z);

    String infoMsg =
        "[uUpdate] state feature num=" + state.localFeatureNum + " admm iteration=" + iter
            + " lbfgs start loss=" + loss;
    LOG.info(infoMsg);

    LBFGS.train(state, lbfgsNumIteration, lbfgsHistory, rho, z, iter);

    loss = LBFGS.getLoss(state, state.x, rho, z);
    infoMsg =
        "[uUpdate] state feature num=" + state.localFeatureNum + " admm iteration=" + iter
            + " lbfgs end loss=" + loss;
    LOG.info(infoMsg);

  }

  public void uUpdate(int lbfgsNumIteration, int lbfgsHistory, double rho, SparseDoubleVector z,
      int iter, ADMMState state) {

    // u_i = u_i + x_i - z_i
    for (int i = 0; i < state.localFeatureNum; i++) {
      state.u[i] = state.u[i] + state.x[i] - z.get(state.local2Global[i]);
    }

    // x update
    double loss = LBFGS.getLoss(state, state.x, rho, z);

    String infoMsg =
        "[uUpdate] state feature num=" + state.localFeatureNum + " admm iteration=" + iter
            + " lbfgs start loss=" + loss;
    LOG.info(infoMsg);

    LBFGS.train(state, lbfgsNumIteration, lbfgsHistory, rho, z, iter);

    loss = LBFGS.getLoss(state, state.x, rho, z);
    infoMsg =
        "[uUpdate] state feature num=" + state.localFeatureNum + " admm iteration=" + iter
            + " lbfgs end loss=" + loss;
    LOG.info(infoMsg);

  }

  public void wUpdate(int iter, ADMMState state) throws Exception {
    String infoMsg = "[wUpdate] admm iteration=" + iter;
    LOG.info(infoMsg);
    DenseDoubleVector update = new DenseDoubleVector(feaNum);
    for (int i = 0; i < state.localFeatureNum; i++) {
      update.set(state.local2Global[i], state.u[i] + state.x[i]);
    }

    update.setMatrixId(wClient.getMatrixId());
    update.setRowId(0);
    wClient.increment(update);
    wClient.clock().get();
  }

  public void tUpdate(TDoubleVector z, int iter, ADMMState state) throws Exception {
    DenseDoubleVector update = new DenseDoubleVector(1);

    double value = 0.0;

    for (int i = 0; i < state.localFeatureNum; i++) {
      int globalId = state.local2Global[i];
      value += Math.pow(state.x[i] - z.get(globalId), 2);
    }

    update.set(0, value);
    update.setMatrixId(tClient.getMatrixId());
    update.setRowId(0);

    tClient.increment(update);
    tClient.clock().get();

    TDoubleVector vector = (TDoubleVector) tClient.getRow(0);

    this.t = vector.get(0);
  }

  public TDoubleVector getW(ADMMState state) throws Exception {
    TDoubleVector w = (TDoubleVector) wClient.getRow(0);
    return w;
  }

  public TDoubleVector getZ(ADMMState state, double lambdaL1, double rho, int N) throws Exception {
    TDoubleVector w = getW(state);
    double kappa = lambdaL1 / (rho * N);
    z.clear();

    for (int i = 0; i < feaNum; i++) {
      double value = w.get(i);
      value = value / N;

      if (value > kappa) {
        z.set(i, value - kappa);
      } else if (value < -kappa) {
        z.set(i, value + kappa);
      }
    }
    return z;
  }

  public void zUpdate(TDoubleVector update) throws Exception {
    String infoMsg = "[zUpdate] admm.";
    LOG.info(infoMsg);

    if (context.getTaskIndex() == 0) {
      update.setMatrixId(zClient.getMatrixId());
      update.setRowId(0);
      zClient.increment(update);
    }

    zClient.clock().get();
  }

  public double getGlobalLoss(ADMMState state, double lambdaL1, double[] z) throws Exception {

    double loss = 0.0;
    for (LabeledData instance : state.instances) {
      SparseDummyVector point = (SparseDummyVector) instance.getX();
      double neg = instance.getY();
      double pos = instance.getY1();

      double score = 0.0;

      int[] indices = point.getIndices();
      for (int i = 0; i < point.getNonzero(); i++) {
        score += state.x[indices[i]];
      }

      double clkLoss = 0.0;
      double nonClkLoss = 0.0;

      if (score < -30) {
        clkLoss = -score;
        nonClkLoss = 0;
      } else if (score > 30) {
        clkLoss = 0;
        nonClkLoss = score;
      } else {
        double temp = 1.0 + Math.exp(-score);
        clkLoss = Math.log(temp);
        nonClkLoss = score + clkLoss;
      }

      loss += pos * clkLoss + neg * nonClkLoss;
    }

    LOG.info("Local lxLoss=" + loss);

    DenseDoubleVector update = new DenseDoubleVector(1);
    update.set(0, loss);
    update.setMatrixId(lossClient.getMatrixId());
    update.setRowId(0);

    lossClient.increment(update);

    lossClient.clock().get();

    TDoubleVector vector = (TDoubleVector) lossClient.getRow(0);
    double lxLoss = vector.get(0);

    LOG.info("Global lxLoss=" + lxLoss);

    double rzLoss = 0.0;
    for (int i = 0; i < feaNum; i++) {
      rzLoss += Math.abs(z[i]);
    }

    LOG.info("Local rzLoss=" + rzLoss);

    rzLoss = lambdaL1 * rzLoss;
    return lxLoss + rzLoss;
  }

  public double getGlobalLoss(ADMMState state, double lambdaL1, SparseDoubleVector z)
      throws Exception {
    double loss = 0.0;

    for (LabeledData instance : state.instances) {
      SparseDummyVector point = (SparseDummyVector) instance.getX();
      double neg = instance.getY();
      double pos = instance.getY1();

      double score = 0.0;

      int[] indices = point.getIndices();
      for (int i = 0; i < point.getNonzero(); i++) {
        score += state.x[indices[i]];
      }

      double clkLoss = 0.0;
      double nonClkLoss = 0.0;

      if (score < -30) {
        clkLoss = -score;
        nonClkLoss = 0;
      } else if (score > 30) {
        clkLoss = 0;
        nonClkLoss = score;
      } else {
        double temp = 1.0 + Math.exp(-score);
        clkLoss = Math.log(temp);
        nonClkLoss = score + clkLoss;
      }

      loss += pos * clkLoss + neg * nonClkLoss;
    }

    LOG.info("Local lxLoss=" + loss);

    DenseDoubleVector update = new DenseDoubleVector(1);
    update.set(0, loss);
    update.setMatrixId(lossClient.getMatrixId());
    update.setRowId(0);

    lossClient.increment(update);

    lossClient.clock().get();

    TDoubleVector vector = (TDoubleVector) lossClient.getRow(0);
    double lxLoss = vector.get(0);

    LOG.info("Global lxLoss=" + lxLoss);

    double rzLoss = 0.0;
    // for (int i = 0; i < feaNum; i++) {
    // rzLoss += Math.abs(z[i]);
    // }
    double[] zval = z.getValues();
    for (int i = 0; i < zval.length; i++) {
      rzLoss += Math.abs(zval[i]);
    }

    LOG.info("Local rzLoss=" + rzLoss);

    rzLoss = lambdaL1 * rzLoss;
    return lxLoss + rzLoss;
  }

}
