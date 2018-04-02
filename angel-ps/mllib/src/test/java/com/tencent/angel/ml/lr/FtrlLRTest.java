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

package com.tencent.angel.ml.lr;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.classification.ftrllr.FTRLLRRunner;
import com.tencent.angel.ml.conf.MLConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

/**
 * FTRL LR UT.
 */
public class FtrlLRTest {
  private static final Log LOG = LogFactory.getLog(FtrlLRTest.class);

  private Configuration conf = new Configuration();
  private static String LOCAL_FS = FileSystem.DEFAULT_FS;
  private static String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  /**
   * set parameter values of conf
   */
  @Before public void setConf() throws Exception {
    try {
      // Feature number of train data
      int featureNum = 124;
      // Total iteration number
      int epochNum = 5;
      // Data format, libsvm or dummy
      String dataFmt = "libsvm";
      // Train batch number per epoch.
      int spPerBatch = 1;
      // Sample ratio
      double spRatio = 1.0;

      double alpha = 1;
      double beta = 1;
      double lambda1 = 0.002;
      double lambda2 = 0.002;

      // Set local deploy mode
      conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");

      // Set basic configuration keys
      conf.setBoolean("mapred.mapper.new-api", true);
      conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
      conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
      conf.set(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, "true");
      conf.setInt(AngelConf.ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS, 100);

      // Set data format
      conf.set(MLConf.ML_DATA_INPUT_FORMAT(), dataFmt);

      //set angel resource parameters #worker, #task, #PS
      conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
      conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1);
      conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);

      //set FTRL LR algorithm parameters #feature #epoch
      conf.set(MLConf.ML_FEATURE_INDEX_RANGE(), String.valueOf(featureNum));
      conf.set(MLConf.ML_EPOCH_NUM(), String.valueOf(epochNum));
      conf.set(MLConf.ML_FTRL_BATCH_SIZE(), String.valueOf(spPerBatch));
      conf.set(MLConf.ML_BATCH_SAMPLE_RATIO(), String.valueOf(spRatio));

      conf.set(MLConf.ML_FTRL_ALPHA(), String.valueOf(alpha));
      conf.set(MLConf.ML_FTRL_BETA(), String.valueOf(beta));
      conf.set(MLConf.ML_FTRL_LAMBDA1(), String.valueOf(lambda1));
      conf.set(MLConf.ML_FTRL_LAMBDA2(), String.valueOf(lambda2));

    } catch (Exception x) {
      LOG.error("setup failed ", x);
      throw x;
    }
  }

  @Test public void testFTRLLR() throws Exception {
    FtrlLRTrainTest();
    FtrlLRIncTrainTest();
    //FtrlLRPredictTest();
  }

  private void FtrlLRTrainTest() throws Exception {
    try {
      String inputPath = "./src/test/data/lr/a9a.train";
      String savePath = LOCAL_FS + TMP_PATH + "/model";
      String logPath = LOCAL_FS + TMP_PATH + "/FtrlLRlog";

      // Set trainning data path
      conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, inputPath);
      // Set save model path
      conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, savePath);
      // Set log path
      conf.set(AngelConf.ANGEL_LOG_PATH, logPath);
      // Set actionType train
      conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_TRAIN());

      FTRLLRRunner runner = new FTRLLRRunner();
      runner.train(conf);
    } catch (Exception x) {
      LOG.error("run trainOnLocalClusterTest failed ", x);
      throw x;
    }
  }

  private void FtrlLRIncTrainTest() throws Exception {
    try {
      String inputPath = "./src/test/data/lr/a9a.train";
      String loadPath = LOCAL_FS + TMP_PATH + "/model";
      String savePath = LOCAL_FS + TMP_PATH + "/newmodel";
      String logPath = LOCAL_FS + TMP_PATH + "/FtrlLRlog";

      // Set trainning data path
      conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, inputPath);
      // Set load model path
      conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, loadPath);
      // Set save model path
      conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, savePath);
      // Set log path
      conf.set(AngelConf.ANGEL_LOG_PATH, logPath);
      // Set actionType incremental train
      conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_INC_TRAIN());

      FTRLLRRunner runner = new FTRLLRRunner();
      runner.incTrain(conf);
    } catch (Exception x) {
      LOG.error("run incTrainTest failed ", x);
      throw x;
    }
  }

  private void FtrlLRPredictTest() throws Exception {
    try {
      String inputPath = "./src/test/data/lr/a9a.test";
      String loadPath = LOCAL_FS + TMP_PATH + "/model";
      String predictPath = LOCAL_FS + TMP_PATH + "/predict";

      // Set predict data path
      conf.set(AngelConf.ANGEL_PREDICT_DATA_PATH, inputPath);
      // Set load model path
      conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, loadPath);
      // Set predict result path
      conf.set(AngelConf.ANGEL_PREDICT_PATH, predictPath);
      // Set actionType train
      conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_PREDICT());

      FTRLLRRunner runner = new FTRLLRRunner();
      runner.predict(conf);
    } catch (Exception x) {
      LOG.error("run predictOnLocalClusterTest failed ", x);
      throw x;
    }
  }
}
