/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.ml.gbdt;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.GBDT.GBDTRunner;
import com.tencent.angel.ml.core.conf.MLConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

/**
 * GBDT UT.
 */
public class GBDTTest {
  private static final Log LOG = LogFactory.getLog(GBDTTest.class);
  private Configuration conf = new Configuration();
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");

  private static final String TrainInputPath = "../../data/agaricus/agaricus_127d_train.libsvm";
  private static final String PredictInputPath = "../../data/agaricus/agaricus_127d_test.libsvm";

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  /**
   * set parameter values of conf
   */
  @Before public void setConf() {
    try {
      // Data format
      String dataType = "libsvm";

      // Feature number of train data
      int featureNum = 127;
      // Tree number
      int treeNum = 2;
      // Tree depth
      int treeDepth = 2;
      // Split number
      int splitNum = 10;
      // Feature sample ratio
      double sampleRatio = 1.0;
      // Ratio of validation
      double validateRatio = 0.1;
      // Learning rate
      double learnRate = 0.01;

      // Set local deploy mode
      conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");

      // Set basic configuration keys
      conf.setBoolean("mapred.mapper.new-api", true);
      conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
      conf.setInt(AngelConf.ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS, 50);
      conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());

      // Set data format
      conf.set(MLConf.ML_DATA_INPUT_FORMAT(), String.valueOf(dataType));

      // Set angel resource, #worker, #task, #PS
      conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
      conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1);
      conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);

      // Set GBDT algorithm parameters
      conf.set(MLConf.ML_FEATURE_INDEX_RANGE(), String.valueOf(featureNum));
      conf.set(MLConf.ML_NUM_TREE(), String.valueOf(treeNum));
      conf.set(MLConf.ML_TREE_MAX_DEPTH(), String.valueOf(treeDepth));
      conf.set(MLConf.ML_TREE_MAX_BIN(), String.valueOf(splitNum));
      conf.set(MLConf.ML_VALIDATE_RATIO(), String.valueOf(validateRatio));
      conf.set(MLConf.ML_TREE_FEATURE_SAMPLE_RATE(), String.valueOf(sampleRatio));
      conf.set(MLConf.ML_LEARN_RATE(), String.valueOf(learnRate));

    } catch (Exception x) {
      LOG.error("setup failed ", x);
      throw x;
    }
  }

  @Test public void testGBDT() throws Exception {
    setConf();
    trainTest();
    predictTest();
  }

  private void trainTest() {
    try {
      String savePath = LOCAL_FS + TMP_PATH + "/model";
      String logPath = LOCAL_FS + TMP_PATH + "/GBDTlog";

      // Set training data path
      conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, TrainInputPath);
      // Set save model path
      conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, savePath);
      // Set log path
      conf.set(AngelConf.ANGEL_LOG_PATH, logPath);
      // Set actionType train
      conf.set(AngelConf.ANGEL_ACTION_TYPE, "train");

      GBDTRunner runner = new GBDTRunner();
      runner.train(conf);
    } catch (Exception x) {
      LOG.error("run trainOnLocalClusterTest failed ", x);
      throw x;
    }
  }

  private void predictTest() {
    try {
      String loadPath = LOCAL_FS + TMP_PATH + "/model";
      String predictPath = LOCAL_FS + TMP_PATH + "/predict";
      String logPath = LOCAL_FS + TMP_PATH + "/GBDTlog";

      // Set predict data path
      conf.set(AngelConf.ANGEL_PREDICT_DATA_PATH, PredictInputPath);
      // Set load model path
      conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, loadPath);
      // Set predict result path
      conf.set(AngelConf.ANGEL_PREDICT_PATH, predictPath);
      // Set log path
      conf.set(AngelConf.ANGEL_LOG_PATH, logPath);
      // Set actionType prediction
      conf.set(AngelConf.ANGEL_ACTION_TYPE, "predict");

      GBDTRunner runner = new GBDTRunner();

      runner.predict(conf);
    } catch (Exception x) {
      LOG.error("run predictTest failed ", x);
      throw x;
    }
  }
}
