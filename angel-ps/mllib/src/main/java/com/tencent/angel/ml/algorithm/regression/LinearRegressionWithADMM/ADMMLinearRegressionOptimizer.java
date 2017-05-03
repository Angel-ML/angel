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

package com.tencent.angel.ml.algorithm.regression.LinearRegressionWithADMM;

import com.tencent.angel.ml.algorithm.optimizer.admm.ADMMSubmitter;
import com.tencent.angel.ml.feature.LabeledData;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.math.vector.DenseIntVector;
import com.tencent.angel.ml.math.vector.SparseDummyVector;
import com.tencent.angel.ml.math.vector.TDoubleVector;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.worker.task.TaskContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class ADMMLinearRegressionOptimizer {

  private static final Log LOG = LogFactory.getLog(ADMMLinearRegressionOptimizer.class);

  private double[] z;
  private double[] zPrev;
  private int feaNum;
  private double t;

  private TaskContext context;

  private final MatrixClient uClient;
  private final MatrixClient wClient;
  private final MatrixClient tClient;
  private final MatrixClient zClient;
  private final MatrixClient lossClient;

  public ADMMLinearRegressionOptimizer(int feaNum, TaskContext context) throws Exception {
    z = new double[feaNum];
    zPrev = new double[feaNum];
    this.feaNum = feaNum;
    t = 0.0;
    this.context = context;

    uClient = context.getMatrix(ADMMSubmitter.AUC);
    wClient = context.getMatrix(ADMMSubmitter.W);
    tClient = context.getMatrix(ADMMSubmitter.T);
    zClient = context.getMatrix(ADMMSubmitter.Z);
    lossClient = context.getMatrix(ADMMSubmitter.Loss);
  }

  public TDoubleVector train(double lambdaL1, double rho, int N, int length, int maxIterNum,
                             int lbfgsNumIteration, ADMMLinearRegressionState state) throws Exception {
    return train(lambdaL1, rho, N, length, maxIterNum, lbfgsNumIteration, 1E-4, 1E-4, 10, state);
  }

  public TDoubleVector train(double lambdaL1, double rho, int N, int length, int maxIterNum,
      int lbfgsNumIteration, double epsilonConv, double epsilonFeas, int lbfgsHistory,
      ADMMLinearRegressionState state) throws Exception {

    for (int iter = 1; iter <= maxIterNum; iter++) {
      LOG.info("[Train] ADMM iteration=" + iter + " start");

      oneRoundComputation(lambdaL1, rho, lbfgsNumIteration, lbfgsHistory, N, iter, state);

      double globalLoss = getGlobalLoss(state, lambdaL1, this.z);
      LOG.info("[Train] ADMM iteration=" + iter + " end loss=" + globalLoss);

      // PredictScore.predict(state, z);

      // double auc = AucData.calcAuc(state, context);

      // LOG.info("[Train] ADMM iteration=" + iter + " train set auc=" + auc);

      if (shouldTerminated(rho, epsilonConv, epsilonFeas, N, iter))
        break;

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

    TDoubleVector z = new DenseDoubleVector(feaNum, this.z);
    zUpdate(z);
    return z;
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
      int lbfgsHistory, int N, int iter, ADMMLinearRegressionState state) throws Exception {
    uUpdate(lbfgsNumIteration, lbfgsHistory, rho, z, iter, state);
    wUpdate(iter, state);
    TDoubleVector z = getZ(state, lambdaL1, rho, N);
    tUpdate(z.getValues(), iter, state);

    RegressionEvaluate.accuracy(state, (DenseDoubleVector) z);
  }

  public void uUpdate(int lbfgsNumIteration, int lbfgsHistory, double rho, double[] z, int iter,
      ADMMLinearRegressionState state) {

    // u_i = u_i + x_i - z_i
    for (int i = 0; i < state.localFeatureNum; i++) {
      state.u[i] = state.u[i] + state.x[i] - z[state.local2Global[i]];
    }

    // x update
    // double loss = LinearRegressionLBFGS.getGradientLoss(state, state.x, rho, z);

    // String infoMsg = "[uUpdate] state feature num=" + state.localFeatureNum
    // + " admm iteration=" + iter + " lbfgs start loss=" + loss;
    // LOG.info(infoMsg);

    LinearRegressionLBFGS.train(state, lbfgsNumIteration, lbfgsHistory, rho, z, iter);

    // loss = LinearRegressionLBFGS.getLoss(state, state.x, rho, z);
    // infoMsg = "[uUpdate] state feature num=" + state.localFeatureNum
    // + " admm iteration=" + iter + " lbfgs end loss=" + loss;
    // LOG.info(infoMsg);

  }

  public void wUpdate(int iter, ADMMLinearRegressionState state) throws Exception {
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

  public void tUpdate(double[] z, int iter, ADMMLinearRegressionState state) throws Exception {
    DenseDoubleVector update = new DenseDoubleVector(1);

    double value = 0.0;

    for (int i = 0; i < state.localFeatureNum; i++) {
      int globalId = state.local2Global[i];
      value += Math.pow(state.x[i] - z[globalId], 2);
    }

    update.set(0, value);
    update.setMatrixId(tClient.getMatrixId());
    update.setRowId(0);

    tClient.increment(update);

    tClient.clock().get();

    TDoubleVector vector = (TDoubleVector) tClient.getRow(0);

    this.t = vector.get(0);
  }

  public TDoubleVector getW(ADMMLinearRegressionState state) throws Exception {
    TDoubleVector w = (TDoubleVector) wClient.getRow(0);
    return w;
  }

  public TDoubleVector getZ(ADMMLinearRegressionState state, double lambdaL1, double rho, int N)
      throws Exception {
    TDoubleVector w = getW(state);
    double kappa = lambdaL1 / (rho * N);

    for (int i = 0; i < feaNum; i++) {
      zPrev[i] = z[i];
      double value = w.get(i);
      value = value / N;
      if (value <= kappa && value >= -kappa) {
        z[i] = 0;
      } else if (value > kappa) {
        z[i] = value - kappa;
      } else if (value < -kappa) {
        z[i] = value + kappa;
      }
    }
    return new DenseDoubleVector(feaNum, z);
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

  public boolean shouldTerminated(double rho, double epsilonConv, double epsilonFeas, int N,
      int iter) {
    double errorConv = 0.0;
    for (int i = 0; i < feaNum; i++) {
      errorConv += (z[i] - zPrev[i]) * (z[i] - zPrev[i]);
    }

    errorConv *= rho * Math.sqrt(N);

    double errorFeas = Math.sqrt(t);

    String infoMsg =
        "[isTerminated] admm iteration=" + iter + " error conv=" + errorConv + " errorFeas="
            + errorFeas;

    LOG.info(infoMsg);

    return errorConv <= epsilonConv && errorFeas <= epsilonFeas;
  }

  public double getGlobalLoss(ADMMLinearRegressionState state, double lambdaL1, double[] z)
      throws Exception {

    double loss = 0.0;
    for (LabeledData instance : state.instances) {
      SparseDummyVector point = (SparseDummyVector) instance.getX();
      double pos = instance.getY();
      double score = 0.0;

      int[] indices = point.getIndices();
      for (int i = 0; i < point.getNonzero(); i++) {
        score += state.x[indices[i]];
      }

      loss += 0.5 * (score - pos) * (score - pos);
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

}
