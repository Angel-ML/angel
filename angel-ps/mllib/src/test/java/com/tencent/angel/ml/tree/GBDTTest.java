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

package com.tencent.angel.ml.tree;

import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.GBDT.GBDTRunner;
import com.tencent.angel.ml.conf.MLConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

public class GBDTTest {

  private static final Log LOG = LogFactory.getLog(GBDTTest.class);
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private static final String inputPath = "./src/test/data/gbdt/agaricus.txt.train";
  private Configuration conf = new Configuration();

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before public void setup() throws Exception {
    try {
      // Feature number of train data
      int featureNum = 127;
      // Number of nonzero features
      int featureNzz = 25;
      // Tree number
      int treeNum = 2;
      // Tree depth
      int treeDepth = 3;
      // Split number
      int splitNum = 10;
      // Feature sample ratio
      double sampleRatio = 1.0;

      // Data format
      String dataFmt = "libsvm";

      // Learning rate
      double learnRate = 0.01;

      // Set basic configuration keys
      conf.setBoolean("mapred.mapper.new-api", true);
      conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);

      // Use local deploy mode and dummy data spliter
      conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");

      // set input, output path
      conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
      conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/LOG/log");

      //set angel resource parameters #worker, #task, #PS
      conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
      conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1);
      conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);

      // Set GBDT algorithm parameters
      conf.set(MLConf.ML_DATA_FORMAT(), dataFmt);
      conf.set(MLConf.ML_FEATURE_NUM(), String.valueOf(featureNum));
      conf.set(MLConf.ML_FEATURE_NNZ(), String.valueOf(featureNzz));
      conf.set(MLConf.ML_GBDT_TREE_NUM(), String.valueOf(treeNum));
      conf.set(MLConf.ML_GBDT_TREE_DEPTH(), String.valueOf(treeDepth));
      conf.set(MLConf.ML_GBDT_SPLIT_NUM(), String.valueOf(splitNum));
      conf.set(MLConf.ML_GBDT_SAMPLE_RATIO(), String.valueOf(sampleRatio));
      conf.set(MLConf.ML_LEARN_RATE(), String.valueOf(learnRate));
    } catch (Exception x) {
      LOG.error("setup failed ", x);
      throw x;
    }
  }

  @Test
  public void testGBDT() throws Exception {
    train();
    predict();
  }

  private void train() throws Exception {
    try {
      // Submit GBDT Train Task
      conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, inputPath);
      conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model");
      GBDTRunner runner = new GBDTRunner();
      runner.train(conf);

      AngelClient angelClient = AngelClientFactory.get(conf);
      angelClient.stop();
    } catch (Exception x) {
      LOG.error("run train failed ", x);
      throw x;
    }
  }

  private void predict() throws Exception {
    try {
      // Load Model from HDFS.
      conf.set(AngelConf.ANGEL_PREDICT_DATA_PATH, inputPath);
      conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model");
      conf.set(AngelConf.ANGEL_PREDICT_PATH, LOCAL_FS + TMP_PATH + "/predict");
      conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_PREDICT());

      conf.set("gbdt.split.feature", TMP_PATH + "/out/xxx");
      conf.set("gbdt.split.value", TMP_PATH + "/out/xxx");

      GBDTRunner runner = new GBDTRunner();

      runner.predict(conf);

      AngelClient angelClient = AngelClientFactory.get(conf);
      angelClient.stop();
    } catch (Exception x) {
      LOG.error("run predict failed ", x);
      throw x;
    }
  }
}
