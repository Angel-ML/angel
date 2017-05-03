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

import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.conf.MatrixConfiguration;
import com.tencent.angel.protobuf.generated.MLProtos;
import com.tencent.angel.AppSubmitter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class ADMMSubmitter implements AppSubmitter {
  private static final Log LOG = LogFactory.getLog(ADMMSubmitter.class);

  // Configuration for ADMM algorithm
  public static final String K_TRAIN_PATH = "angel.admmlr.trainPath";
  public static final String K_L1_NORM = "angel.admmlr.norm";
  public static final String K_ADMM_ITER = "angel.admmlr.iter";
  public static final String K_LBFGS_ITER = "angel.admmlr.lbfgsIter";
  public static final String K_RHO = "angel.admmlr.rho";
  // public static final String K_MODLE_OUTPUT = "angel.admmlr.modelOutput";
  public static final String K_FEATURE_NUM = "angel.admmlr.feaNum";
  public static final String K_MAXDIM_PATH = "angel.lr.admm.max.dim.path";
  public static final String K_CONF_PATH = "angel.admmlr.confPath";

  public static final String W = "W";
  public static final String T = "t";
  public static final String Z = "z";
  public static final String Loss = "loss";
  public static final String AUC = "auc";

  @Override
  public void submit(Configuration conf) throws Exception {
    conf.setBoolean(AngelConfiguration.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);

    AngelClient client = AngelClientFactory.get(conf);

    String action = conf.get(AngelConfiguration.ANGEL_ACTION_TYPE);
    if (action.matches("train")) {
      String path = conf.get(K_CONF_PATH, null);
      if (path != null) {
        conf.addResource(new Path(path));
      }

      String trainPath = conf.get(K_TRAIN_PATH, null);
      // String modelOutput = conf.get(K_MODLE_OUTPUT, null);

      conf.set(AngelConfiguration.ANGEL_TRAIN_DATA_PATH, trainPath);
      // conf.set(AngelConfiguration.ANGEL_JOB_OUTPUT_PATH, modelOutput);

      int admmIter = conf.getInt(K_ADMM_ITER, 20);
      conf.setInt(AngelConfiguration.ANGEL_TASK_ITERATION_NUMBER, admmIter);

      int psNum = conf.getInt(AngelConfiguration.ANGEL_PS_NUMBER, 1);

      int feaNum = 0;
      String maxdimPathStr = conf.get(K_MAXDIM_PATH);
      if (maxdimPathStr != null) {
        Path maxdimPath = new Path(maxdimPathStr);
        FileSystem fs = maxdimPath.getFileSystem(conf);
        FSDataInputStream fin = fs.open(maxdimPath);
        LineReader lr = new LineReader(fin);
        Text line = new Text();
        lr.readLine(line);
        try {
          feaNum = Integer.valueOf(line.toString());
        } catch (Exception x) {
          feaNum = conf.getInt(K_FEATURE_NUM, 1000000);
        }
        lr.close();
      } else {
        feaNum = conf.getInt(K_FEATURE_NUM, 1000000);
      }
      // int feaNum = conf.getInt(K_FEATURE_NUM, 1000000);

      LOG.info("MAX_DIM=" + feaNum);
      LOG.info("ANGEL_TRAIN_DATA_PATH=" + conf.get(AngelConfiguration.ANGEL_TRAIN_DATA_PATH));
      LOG.info("ANGEL_OUTPUT_DIR=" + conf.get(AngelConfiguration.ANGEL_JOB_OUTPUT_PATH));
      LOG.info("FEATURE_NUM=" + feaNum);
      LOG.info("ADMM_ITER=" + admmIter);

      MatrixContext mContext1 = new MatrixContext(W, 1, feaNum, 1, feaNum / psNum / 10000);
      client.addMatrix(mContext1);

      // Z
      MatrixContext mContext2 = new MatrixContext(Z, 1, feaNum, 1, feaNum / psNum / 10000);
      mContext2.setRowType(MLProtos.RowType.T_DOUBLE_SPARSE);
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
    } else {
      String trainPath = conf.get(K_TRAIN_PATH, null);
      conf.set(AngelConfiguration.ANGEL_TRAIN_DATA_PATH, trainPath);
      conf.set(AngelConfiguration.ANGEL_PS_NUMBER, String.valueOf(1));
      MatrixContext tmpMatrix = new MatrixContext();
      tmpMatrix.setName("tmp");
      tmpMatrix.setRowNum(1);
      tmpMatrix.setColNum(10);
      tmpMatrix.setMaxRowNumInBlock(1);
      tmpMatrix.setMaxColNumInBlock(10);
      tmpMatrix.setRowType(MLProtos.RowType.T_DOUBLE_DENSE);
      tmpMatrix.set(MatrixConfiguration.MATRIX_AVERAGE, "true");
      tmpMatrix.set(MatrixConfiguration.MATRIX_HOGWILD, String.valueOf(true));
      client.addMatrix(tmpMatrix);
    }

    client.start();
    client.waitForCompletion();
    client.stop();
  }

}
