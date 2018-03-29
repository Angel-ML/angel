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

package com.tencent.angel.ml.metric;

import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.worker.task.TaskId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AlgoMetricsTest {
  private static final Log LOG = LogFactory.getLog(AlgoMetricsTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private AngelClient angelClient;
  private TaskId task0Id;
  private TaskId task1Id;

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before public void setup() throws Exception {
    try {
      // set basic configuration keys
      Configuration conf = new Configuration();
      conf.setBoolean("mapred.mapper.new-api", true);
      conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);

      // use local deploy mode and dummy dataspliter
      conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");
      conf.setBoolean(AngelConf.ANGEL_AM_USE_DUMMY_DATASPLITER, true);
      conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
      conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/out");
      conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, LOCAL_FS + TMP_PATH + "/in");
      conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/log");

      conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
      conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);
      conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 2);

      // get a angel client
      angelClient = AngelClientFactory.get(conf);

      // add matrix
      MatrixContext mMatrix = new MatrixContext();
      mMatrix.setName("w1");
      mMatrix.setRowNum(1);
      mMatrix.setColNum(100000);
      mMatrix.setMaxRowNumInBlock(1);
      mMatrix.setMaxColNumInBlock(50000);
      mMatrix.setRowType(RowType.T_INT_DENSE);
      mMatrix.set(MatrixConf.MATRIX_OPLOG_ENABLEFILTER, "false");
      mMatrix.set(MatrixConf.MATRIX_HOGWILD, "true");
      mMatrix.set(MatrixConf.MATRIX_AVERAGE, "false");
      mMatrix.set(MatrixConf.MATRIX_OPLOG_TYPE, "DENSE_INT");
      angelClient.addMatrix(mMatrix);

      angelClient.startPSServer();
      angelClient.runTask(MetricTestTask.class);
      Thread.sleep(5000);
      task0Id = new TaskId(0);
      task1Id = new TaskId(1);
    } catch (Exception x) {
      LOG.error("setup failed ", x);
      throw x;
    }
  }

  @Test public void testAlgo() throws Exception {
    try {
      angelClient.waitForCompletion();
    } catch (Exception x) {
      LOG.error("run testAlgo failed. ", x);
      throw x;
    }
  }

  @After public void stop() throws Exception {
    try {
      LOG.info("stop local cluster");
      angelClient.stop();
    } catch (Exception x) {
      LOG.error("stop failed ", x);
      throw x;
    }
  }
}
