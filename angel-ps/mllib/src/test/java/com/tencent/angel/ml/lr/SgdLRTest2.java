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
import com.tencent.angel.ml.classification2.lr.LRRunner;
import com.tencent.angel.ml.conf.MLConf;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ml.optimizer2.OptMethods;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

/**
 * Gradient descent LR UT.
 */
public class SgdLRTest2 {
  private Configuration conf = new Configuration();
  private static final Log LOG = LogFactory.getLog(SgdLRTest2.class);
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
      // number of mini batch within a update periorid
      int updatePerEpoch = 2;
      // Data format, libsvm or dummy
      String dataFmt = "libsvm";
      // Batch size
      int batchSize = 128;
      // Model type
      String modelType = String.valueOf(RowType.T_DOUBLE_DENSE);

      // Learning rate
      double learnRate = 1.0;
      // Decay of learning rate
      double decay = 1;
      // Regularization coefficient
      double reg1 = 0.01;
      double reg2 = 0.01;

      // Set local deploy mode
      conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");

      // Set basic configuration keys
      conf.setBoolean("mapred.mapper.new-api", true);
      conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
      conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
      conf.set(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, "true");
      conf.setInt(AngelConf.ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS, 100);

      // conf.setBoolean(MLConf.ML_INDEX_GET_ENABLE(), true);
      // Set data format
      conf.set(MLConf.ML_DATA_INPUT_FORMAT(), dataFmt);

      //set angel resource parameters #worker, #task, #PS
      conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
      conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1);
      conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);

      //set sgd LR algorithm parameters #feature #epoch
      conf.set(MLConf.ML_MODEL_TYPE(), modelType);
      conf.set(MLConf.ML_FEATURE_INDEX_RANGE(), String.valueOf(featureNum));
      conf.set(MLConf.ML_EPOCH_NUM(), String.valueOf(epochNum));
      conf.set(MLConf.ML_LEARN_RATE(), String.valueOf(learnRate));
      conf.set(MLConf.ML_LEARN_DECAY(), String.valueOf(decay));
      conf.set(MLConf.ML_LR_REG_L1(), String.valueOf(reg1));
      conf.set(MLConf.ML_LR_REG_L2(), String.valueOf(reg2));
      conf.set(MLConf.ML_MINIBATCH_SIZE(), String.valueOf(batchSize));
      conf.set(MLConf.ML_NUM_UPDATE_PER_EPOCH(), String.valueOf(updatePerEpoch));
    } catch (Exception x) {
      LOG.error("setup failed ", x);
      throw x;
    }
  }


  @Test public void trainOnLocalClusterTest() throws Exception {
    try {
      String inputPath = "./src/test/data/lr/a9a.train";
      String savePath = LOCAL_FS + TMP_PATH + "/model";
      String logPath = LOCAL_FS + TMP_PATH + "/LRlog";

      // Set trainning data path
      conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, inputPath);
      // Set save model path
      conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, savePath);
      // Set log path
      conf.set(AngelConf.ANGEL_LOG_PATH, logPath);
      // Set actionType train
      conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_TRAIN());
      conf.set(MLConf.ML_LEARN_RATE(), String.valueOf(0.05));

      String modelType = String.valueOf(RowType.T_DOUBLE_DENSE);
      conf.set(MLConf.ML_MODEL_TYPE(), modelType);

      conf.set(MLConf.ML_OPT_METHOD(), OptMethods.AdaGrad());

      LRRunner runner = new LRRunner();
      runner.train(conf);
    } catch (Exception x) {
      LOG.error("run trainOnLocalClusterTest failed ", x);
      throw x;
    }
  }


  @Test public void predictTest() throws Exception {
    try {
      String inputPath = "./src/test/data/lr/a9a.test";
      String loadPath = LOCAL_FS + TMP_PATH + "/model";
      String predictPath = LOCAL_FS + TMP_PATH + "/predict";

      // Set trainning data path
      conf.set(AngelConf.ANGEL_PREDICT_DATA_PATH, inputPath);
      // Set load model path
      conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, loadPath);
      // Set predict result path
      conf.set(AngelConf.ANGEL_PREDICT_PATH, predictPath);
      // Set actionType prediction
      conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_PREDICT());
      LRRunner runner = new LRRunner();

      runner.predict(conf);
    } catch (Exception x) {
      LOG.error("run predictTest failed ", x);
      throw x;
    }
  }
}
