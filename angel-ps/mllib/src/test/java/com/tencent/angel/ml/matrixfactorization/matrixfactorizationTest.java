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

package com.tencent.angel.ml.matrixfactorization;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.conf.MLConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)

public class matrixfactorizationTest {
  private static final Log LOG = LogFactory.getLog(matrixfactorizationTest.class);
  private Configuration conf = new Configuration();

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Test public void testMF() throws Exception {
    try {
      String inputPath = "./src/test/data/recommendation/MovieLensDataSet";

      // Set local deploy mode
      conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");
      // Set basic configuration keys
      conf.setBoolean("mapred.mapper.new-api", true);
      conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
      conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
      conf.setInt(AngelConf.ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS, 100);

      //set angel resource parameters #worker, #task, #PS
      conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
      conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1);
      conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);

      String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
      String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");

      // Set trainning data, save model, log path
      conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, inputPath);
      conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model");
      conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/Log/log");
      conf.set(MLConf.ML_MF_USER_MODEL_OUTPUT_PATH(), LOCAL_FS + TMP_PATH + "/usermodel");
      // Set actionType train
      conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_TRAIN());

      // Set MF algorithm parameters
      conf.set(MLConf.ML_MF_RANK(), "200");
      conf.set(MLConf.ML_EPOCH_NUM(), "5");
      conf.set(MLConf.ML_MF_ROW_BATCH_NUM(), "2");
      conf.set(MLConf.ML_MF_ITEM_NUM(), "1683");
      conf.set(MLConf.ML_MF_LAMBDA(), "0.01");
      conf.set(MLConf.ML_MF_ETA(), "0.0005");

      MatrixFactorizationRunner runner = new MatrixFactorizationRunner();
      runner.train(conf);
    } catch (Exception x) {
      LOG.error("run testMF failed ", x);
      throw x;
    }
  }
}
