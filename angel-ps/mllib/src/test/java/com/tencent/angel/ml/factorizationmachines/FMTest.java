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

package com.tencent.angel.ml.factorizationmachines;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.conf.MLConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

public class FMTest {
  private Configuration conf = new Configuration();

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  /**
   * set parameter values of conf
   */
  @Before public void setConf() {

    // Feature number of train data
    int featureNum = 236;
    // Total iteration number
    int epochNum = 5;
    // Rank
    int rank = 3;
    // Regularization parameters
    double reg0 = 0.0;
    double reg1 = 0.00001;
    double reg2 = 0.00005;
    // Learn rage
    double lr = 1.0;
    double stev = 0.0001;

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

    //set FM algorithm parameters #feature #epoch
    conf.set(MLConf.ML_FEATURE_INDEX_RANGE(), String.valueOf(featureNum));
    conf.set(MLConf.ML_EPOCH_NUM(), String.valueOf(epochNum));
    conf.set(MLConf.ML_FM_RANK(), String.valueOf(rank));
    conf.set(MLConf.ML_LEARN_RATE(), String.valueOf(lr));
    conf.set(MLConf.ML_FM_REG_L2_W(), String.valueOf(reg1));
    conf.set(MLConf.ML_FM_REG_L2_V(), String.valueOf(reg2));
    conf.set(MLConf.ML_FM_V_STDDEV(), String.valueOf(stev));
  }

  @Test
  public void testFM() throws Exception {
    trainOnLocalClusterTest();
    FMClassificationTest();
    FMPredictTest();
  }

  public void trainOnLocalClusterTest() throws Exception {
    String inputPath = "./src/test/data/fm/food_fm_libsvm";
    String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
    String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
    String savePath = LOCAL_FS + TMP_PATH + "/model";
    String logPath = LOCAL_FS + TMP_PATH + "/FMlog";

    // Set trainning data path
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, inputPath);
    // Set save model path
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, savePath);
    // Set log path
    conf.set(AngelConf.ANGEL_LOG_PATH, logPath);
    // Set actionType train
    conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_TRAIN());
    // Set feature number
    conf.set(MLConf.ML_DATA_INPUT_FORMAT(), "libsvm");

    FMRunner runner = new FMRunner();
    runner.train(conf);
  }

  public void FMClassificationTest() throws Exception {
    String inputPath = "./src/test/data/fm/a9a.train.dummy";
    String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
    String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
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
    conf.set(MLConf.ML_DATA_INPUT_FORMAT(), "dummy");
    // Set learnType
    conf.set(MLConf.ML_FM_LEARN_TYPE(), "c");
    // Set feature number
    conf.set(MLConf.ML_FEATURE_INDEX_RANGE(), String.valueOf(124));
    // Set
    conf.set(MLConf.ML_POSITIVE_SAMPLE_WEIGHT(), "1.0");
    conf.set(MLConf.ML_NEGATIVE_SAMPLE_WEIGHT(), "0.5");
    conf.set(MLConf.ML_LEARN_RATE(), "0.000001");
    conf.set(MLConf.ML_FM_V_STDDEV(), "0.000001");
    conf.set(MLConf.ML_EPOCH_NUM(), "10");
    conf.set(AngelConf.ANGEL_WORKERGROUP_NUMBER, "1");
    conf.set(MLConf.ML_LEARN_RATE(), String.valueOf(0.01));

    FMRunner runner = new FMRunner();
    runner.train(conf);
  }

  public void FMPredictTest() throws Exception {
    String inputPath = "./src/test/data/fm/a9a.train.dummy";
    String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
    String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
    String savePath = LOCAL_FS + TMP_PATH + "/model";
    String logPath = LOCAL_FS + TMP_PATH + "/FMlog";
    String outPath = LOCAL_FS + TMP_PATH + "/FMPredictOut";

    // Set trainning data path
    conf.set(AngelConf.ANGEL_PREDICT_DATA_PATH, inputPath);
    // Set save model path
    conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, savePath);
    // Set log path
    conf.set(AngelConf.ANGEL_LOG_PATH, logPath);
    // Set FM predict output path
    conf.set(AngelConf.ANGEL_PREDICT_PATH, outPath);
    // Set actionType train
    conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_PREDICT());
    conf.set(MLConf.ML_DATA_INPUT_FORMAT(), "dummy");
    // Set learnType
    conf.set(MLConf.ML_FM_LEARN_TYPE(), "c");
    // Set feature number
    conf.set(MLConf.ML_FEATURE_INDEX_RANGE(), String.valueOf(124));
    conf.set(AngelConf.ANGEL_WORKERGROUP_NUMBER, "1");

    FMRunner runner = new FMRunner();
    runner.predict(conf);

    System.out.println(outPath);
  }
}

