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

package com.tencent.angel.ml.optimizer.sgd;

import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.conf.MatrixConfiguration;
import com.tencent.angel.ml.conf.MLConf;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.protobuf.generated.MLProtos;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class GradientDescentTest {
  private static final Log LOG = LogFactory.getLog(GradientDescentTest.class);
  private static final String inputPath = "./src/test/data/LinearRegression/LinearReg100.train";
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private static final String logPath = "./src/test/log";
  private static AngelClient angelClient;
  private static Configuration conf;
  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before
  public void setUp() throws Exception {
    // set basic configuration keys
    conf = new Configuration();
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean(AngelConfiguration.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
    conf.set(AngelConfiguration.ANGEL_TASK_USER_TASKCLASS, DummyTask.class.getName());

    // use local deploy mode and dummy dataspliter
    conf.set(AngelConfiguration.ANGEL_DEPLOY_MODE, "LOCAL");
    conf.setBoolean(AngelConfiguration.ANGEL_AM_USE_DUMMY_DATASPLITER, true);
    // conf.setInt(AngelConfiguration.ANGEL_PREPROCESS_VECTOR_MAXDIM, 10000);
    conf.set(AngelConfiguration.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
    // conf.set(AngelConfiguration.ANGEL_MODEL_PATH, LOCAL_FS + TMP_PATH + "/out");
    conf.set(FileInputFormat.INPUT_DIR, LOCAL_FS + TMP_PATH + "/in");

    // Set trainning data path
    conf.set(AngelConfiguration.ANGEL_TRAIN_DATA_PATH, inputPath);
    // Set save model path
    conf.set(AngelConfiguration.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model");
    // Set log path
    conf.set(AngelConfiguration.ANGEL_LOG_PATH, logPath);
    // Set actionType train
    conf.set(MLConf.ANGEL_ACTION_TYPE(), MLConf.ANGEL_ML_TRAIN());

    conf.setInt(AngelConfiguration.ANGEL_WORKERGROUP_NUMBER, 1);
    conf.setInt(AngelConfiguration.ANGEL_PS_NUMBER, 1);
    conf.setInt(AngelConfiguration.ANGEL_WORKER_TASK_NUMBER, 2);

    // get a angel client
    angelClient = AngelClientFactory.get(conf);
    angelClient.startPSServer();
    // add matrix
    MatrixContext mMatrix = new MatrixContext();
    mMatrix.setName("w1");
    mMatrix.setRowNum(1);
    mMatrix.setColNum(100000);
    mMatrix.setMaxRowNumInBlock(1);
    mMatrix.setMaxColNumInBlock(50000);
    mMatrix.setRowType(MLProtos.RowType.T_INT_DENSE);
    mMatrix.set(MatrixConfiguration.MATRIX_OPLOG_ENABLEFILTER, "false");
    mMatrix.set(MatrixConfiguration.MATRIX_HOGWILD, "true");
    mMatrix.set(MatrixConfiguration.MATRIX_AVERAGE, "false");
    mMatrix.set(MatrixConfiguration.MATRIX_OPLOG_TYPE, "DENSE_INT");
    angelClient.addMatrix(mMatrix);

    mMatrix.setName("w2");
    mMatrix.setRowNum(1);
    mMatrix.setColNum(100000);
    mMatrix.setMaxRowNumInBlock(1);
    mMatrix.setMaxColNumInBlock(50000);
    mMatrix.setRowType(MLProtos.RowType.T_DOUBLE_DENSE);
    mMatrix.set(MatrixConfiguration.MATRIX_OPLOG_ENABLEFILTER, "false");
    mMatrix.set(MatrixConfiguration.MATRIX_HOGWILD, "false");
    mMatrix.set(MatrixConfiguration.MATRIX_AVERAGE, "false");
    mMatrix.set(MatrixConfiguration.MATRIX_OPLOG_TYPE, "DENSE_DOUBLE");
    angelClient.addMatrix(mMatrix);
    angelClient.runTask(DummyTask.class);
    Thread.sleep(5000);
  }

  @After
  public void tearDown() throws Exception {
    LOG.info("stop local cluster");
    angelClient.stop();
  }

  @Test
  public void testRunMiniBatchL2SGD() throws Exception {

  }

  @Test
  public void testRunMiniBatchSGD() throws Exception {

  }

  @Test
  public void testRunMiniL2BatchSGD() throws Exception {

  }

  @Test
  public void testRunMiniL2BatchSGD1() throws Exception {

  }

  @Test
  public void testRunMiniL1BatchSGD() throws Exception {

  }

  @Test
  public void testTruncGradient() throws Exception {

  }
}
