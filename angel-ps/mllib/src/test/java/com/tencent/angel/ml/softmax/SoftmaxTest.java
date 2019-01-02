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


package com.tencent.angel.ml.softmax;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.core.conf.MLConf;
import com.tencent.angel.ml.core.graphsubmit.GraphRunner;
import com.tencent.angel.ml.matrix.RowType;
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
public class SoftmaxTest {
  private Configuration conf = new Configuration();
  private static final Log LOG = LogFactory.getLog(SoftmaxTest.class);
  private static String LOCAL_FS = FileSystem.DEFAULT_FS;
  private static String CLASSBASE = "com.tencent.angel.ml.classification.";
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
      int featureNum = 357;
      // Total iteration number
      int epochNum = 5;
      // Validation sample Ratio
      double vRatio = 0.1;
      // Data format, libsvm or dummy
      String dataFmt = "libsvm";
      // class number
      int classNum = 3;
      // Model type
      String modelType = String.valueOf(RowType.T_DOUBLE_DENSE);

      // Learning rate
      double learnRate = 0.5;
      // Decay of learning rate
      double decay = 1;
      // Regularization coefficient
      double reg = 0.002;

      // Set local deploy mode
      conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");

      // Set basic configuration keys
      conf.setBoolean("mapred.mapper.new-api", true);
      conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
      conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
      conf.setInt(AngelConf.ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS, 10);
      conf.setInt(AngelConf.ANGEL_WORKER_HEARTBEAT_INTERVAL_MS, 1000);
      conf.setInt(AngelConf.ANGEL_PS_HEARTBEAT_INTERVAL_MS, 1000);

      // Set data format
      conf.set(MLConf.ML_DATA_INPUT_FORMAT(), dataFmt);

      //set angel resource parameters #worker, #task, #PS
      conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
      conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1);
      conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);
      conf.setBoolean(MLConf.ML_DATA_IS_NEGY(), false);

      //set sgd LR algorithm parameters #feature #epoch
      conf.set(MLConf.ML_MODEL_TYPE(), modelType);
      conf.set(MLConf.ML_FEATURE_INDEX_RANGE(), String.valueOf(featureNum));
      conf.set(MLConf.ML_EPOCH_NUM(), String.valueOf(epochNum));
      conf.set(MLConf.ML_VALIDATE_RATIO(), String.valueOf(vRatio));
      conf.set(MLConf.ML_LEARN_RATE(), String.valueOf(learnRate));
      conf.set(MLConf.ML_OPT_DECAY_ALPHA(), String.valueOf(decay));
      conf.set(MLConf.ML_REG_L2(), String.valueOf(reg));
      conf.setLong(MLConf.ML_MODEL_SIZE(), featureNum);
      conf.setInt(MLConf.ML_NUM_CLASS(), classNum);
      conf.setBoolean(MLConf.ML_DATA_USE_SHUFFLE(), true);
      conf.set(MLConf.ML_MODEL_CLASS_NAME(), CLASSBASE + "SoftmaxRegression");
    } catch (Exception x) {
      LOG.error("setup failed ", x);
      throw x;
    }
  }

  @Test public void testSoftMax() throws Exception {
    setConf();
    trainTest();
    incTrain();
    predictTest();
  }

  private void trainTest() throws Exception {
    try {
      String inputPath = "../../data/protein/protein_357d_train.libsvm";
      String savePath = LOCAL_FS + TMP_PATH + "/SoftMax";
      String logPath = LOCAL_FS + TMP_PATH + "/SoftMaxlog";

      // Set log path
      conf.set(AngelConf.ANGEL_LOG_PATH, logPath);

      // Set trainning data path
      conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, inputPath);
      // Set save model path
      conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, savePath);

      // Set actionType train
      conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_TRAIN());

      GraphRunner runner = new GraphRunner();
      runner.train(conf);
    } catch (Exception x) {
      LOG.error("run trainOnLocalClusterTest failed ", x);
      throw x;
    }
  }


  private void incTrain() {
    try {
      String inputPath = "../../data/protein/protein_357d_train.libsvm";
      String savePath = LOCAL_FS + TMP_PATH + "/SoftMax";
      String logPath = LOCAL_FS + TMP_PATH + "/SoftMaxlog";
      String newPath = LOCAL_FS + TMP_PATH + "/NewSoftMax";

      // Set trainning data path
      conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, inputPath);
      // Set load model path
      conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, savePath);
      // Set save model path
      conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, newPath);
      // Set actionType incremental train
      conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_INC_TRAIN());
      // Set log path
      conf.set(AngelConf.ANGEL_LOG_PATH, logPath);


      GraphRunner runner = new GraphRunner();
      runner.train(conf);
    } catch (Exception e) {
      LOG.error("run incTrainTest failed", e);
      throw e;
    }
  }

  private void predictTest() throws Exception {
    try {
      String inputPath = "../../data/protein/protein_357d_test.libsvm";
      String loadPath = LOCAL_FS + TMP_PATH + "/SoftMax";
      String predictPath = LOCAL_FS + TMP_PATH + "/predict";

      // Set trainning data path
      conf.set(AngelConf.ANGEL_PREDICT_DATA_PATH, inputPath);
      // Set load model path
      conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, loadPath);
      // Set predict result path
      conf.set(AngelConf.ANGEL_PREDICT_PATH, predictPath);

      conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_PREDICT());

      GraphRunner runner = new GraphRunner();

      runner.predict(conf);
    } catch (Exception x) {
      LOG.error("run predictTest failed ", x);
      throw x;
    }
  }
}
