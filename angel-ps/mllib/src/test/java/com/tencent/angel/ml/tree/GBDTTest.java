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
  private static final String trainInputPath = "./src/test/data/gbdt/agaricus.txt.train";
  private static final String testInputPath = "./src/test/data/gbdt/agaricus.txt.test";
  private Configuration conf = new Configuration();

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before public void setup() throws Exception {
    try {

      conf.set(MLConf.ML_GBDT_TASK_TYPE(), "classification");

      // Feature number of train data
      int featureNum = 127;
      // Number of nonzero features
      int featureNzz = 25;
      // Tree number
      int treeNum = 5;
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
      conf.setInt(AngelConf.ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS, 100);

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
      conf.set(MLConf.ML_DATA_INPUT_FORMAT(), dataFmt);
      conf.set(MLConf.ML_FEATURE_INDEX_RANGE(), String.valueOf(featureNum));
      conf.set(MLConf.ML_GBDT_TREE_NUM(), String.valueOf(treeNum));
      conf.set(MLConf.ML_GBDT_TREE_DEPTH(), String.valueOf(treeDepth));
      conf.set(MLConf.ML_GBDT_SPLIT_NUM(), String.valueOf(splitNum));
      conf.set(MLConf.ML_GBDT_SAMPLE_RATIO(), String.valueOf(sampleRatio));
      conf.set(MLConf.ML_LEARN_RATE(), String.valueOf(learnRate));
      conf.set(MLConf.ML_GBDT_CATE_FEAT(),
        "0:2,1:2,2:2,3:2,4:2,5:2,6:2,7:2,8:2,9:2,10:2,11:2,12:2,13:2,14:2,15:2,16:2,17:2,18:2,19:2,20:2,21:2,22:2,23:2,24:2,25:2,26:2,27:2,28:2,29:2,30:2,31:2,32:2,33:2,34:2,35:2,36:2,37:2,38:2,39:2,40:2,41:2,42:2,43:2,44:2,45:2,46:2,47:2,48:2,49:2,50:2,51:2,52:2,53:2,54:2,55:2,56:2,57:2,58:2,59:2,60:2,61:2,62:2,63:2,64:2,65:2,66:2,67:2,68:2,69:2,70:2,71:2,72:2,73:2,74:2,75:2,76:2,77:2,78:2,79:2,80:2,81:2,82:2,83:2,84:2,85:2,86:2,87:2,88:2,89:2,90:2,91:2,92:2,93:2,94:2,95:2,96:2,97:2,98:2,99:2,100:2,101:2,102:2,103:2,104:2,105:2,106:2,107:2,108:2,109:2,110:2,111:2,112:2,113:2,114:2,115:2,116:2,117:2,118:2,119:2,120:2,121:2,122:2,123:2,124:2,125:2,126:2");
    } catch (Exception x) {
      LOG.error("setup failed ", x);
      throw x;
    }
  }

  @Test public void testGBDT() throws Exception {
    train();
    predict();
  }

  private void train() throws Exception {
    try {
      // Submit GBDT Train Task
      conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, trainInputPath);
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
      conf.set(AngelConf.ANGEL_PREDICT_DATA_PATH, testInputPath);
      conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model");
      conf.set(AngelConf.ANGEL_PREDICT_PATH, LOCAL_FS + TMP_PATH + "/predict");
      conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_PREDICT());

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
