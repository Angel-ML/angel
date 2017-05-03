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

import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.algorithm.optimizer.admm.AucData;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.conf.MatrixConfiguration;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.AppSubmitter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class ADMMLinearRegressionSubmitter implements AppSubmitter {

  private static final Log LOG = LogFactory.getLog(ADMMLinearRegressionSubmitter.class);

  // Configuration for ADMM algorithm
  public static final String K_TRAIN_PATH = "angel.admm.linear.regression.trainPath";
  public static final String K_L1_NORM = "angel.admm.linear.regression.norm";
  public static final String K_ADMM_ITER = "angel.admm.linear.regression.iter";
  public static final String K_LBFGS_ITER = "angel.admm.linear.regression.lbfgsIter";
  public static final String K_RHO = "angel.admm.linear.regression.rho";
  public static final String K_MODLE_OUTPUT = "angel.admm.linear.regression.output";
  public static final String K_FEATURE_NUM = "angel.admm.linear.regression.features";

  public static final String K_CONF_PATH = "angel.admm.linear.regression.confPath";

  public static final String W = "W";
  public static final String T = "t";
  public static final String Z = "z";
  public static final String Loss = "loss";
  public static final String AUC = "auc";

  @Override
  public void submit(Configuration conf) throws Exception {
    String path = conf.get(K_CONF_PATH, null);
    if (path != null) {
      conf.addResource(new Path(path));
    }

    String trainPath = conf.get(K_TRAIN_PATH, null);
    String modelOutput = conf.get(K_MODLE_OUTPUT, null);

    int admmIter = conf.getInt(K_ADMM_ITER, 20);
    int lbfgsIter = conf.getInt(K_LBFGS_ITER, 5);
    int feaNum = conf.getInt(K_FEATURE_NUM, 10000000);

    double l1Norm = conf.getDouble(K_L1_NORM, 5);
    double rho = conf.getDouble(K_RHO, 1.0);

    conf.set(AngelConfiguration.ANGEL_TRAIN_DATA_PATH, trainPath);
    conf.set(AngelConfiguration.ANGEL_JOB_OUTPUT_PATH, modelOutput);
    conf.setInt(AngelConfiguration.ANGEL_TASK_ITERATION_NUMBER, admmIter);

    int psNum = conf.getInt(AngelConfiguration.ANGEL_PS_NUMBER, 1);

    // conf.setClass(AngelConfiguration.ANGEL_TASK_USER_PREPROCESS_CLASS,
    // ADMMLinearRegressionPreprocessor.class, PreProcessor.class);

    LOG.info("ANGEL_INPUT_DIR=" + conf.get(AngelConfiguration.ANGEL_TRAIN_DATA_PATH));
    LOG.info("ANGEL_OUTPUT_DIR=" + conf.get(AngelConfiguration.ANGEL_JOB_OUTPUT_PATH));
    LOG.info("FEATURE_NUM=" + feaNum);
    LOG.info("ADMM_ITER=" + admmIter);
    LOG.info("LBFGS_ITER=" + lbfgsIter);
    LOG.info("L1_NORM=" + l1Norm);
    LOG.info("RHO=" + rho);

    AngelClient client = AngelClientFactory.get(conf);

    MatrixContext mContext1 = new MatrixContext(W, 1, feaNum, 1, feaNum / psNum / 10);
    client.addMatrix(mContext1);

    // Z
    MatrixContext mContext2 = new MatrixContext(Z, 1, feaNum, 1, feaNum / psNum);
    // mContext2.set(MatrixConfiguration.MATRIX_AVERAGE, String.valueOf(true));
    client.addMatrix(mContext2);

    // T
    MatrixContext mContext3 = new MatrixContext(T, 1, 1, 1, 1);
    client.addMatrix(mContext3);

    MatrixContext mContext4 = new MatrixContext(Loss, 1, 1, 1, 1);
    mContext4.setRowType(MLProtos.RowType.T_DOUBLE_DENSE);
    client.addMatrix(mContext4);

    MatrixContext mContext5 =
        new MatrixContext(AUC, 1, AucData.bucketNum * 2, 1, AucData.bucketNum / psNum / 10);
    mContext5.setRowType(MLProtos.RowType.T_INT_DENSE);
    mContext5.set(MatrixConfiguration.MATRIX_OPLOG_TYPE, "DENSE_INT");
    client.addMatrix(mContext5);

    client.start();
    client.waitForCompletion();
    client.stop();
  }

}
