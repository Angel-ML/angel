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

package com.tencent.angel.ml.kmeans;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.clustering.kmeans.KMeansRunner;
import com.tencent.angel.ml.conf.MLConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

public class KmeansTest {
  private static final Log LOG = LogFactory.getLog(KmeansTest.class);
  private Configuration conf = new Configuration();
  private static final String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
  private static final String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  private static final String inputPath = "./src/test/data/clustering/iris";

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  @Before public void setup() throws Exception {
    try {
      String dataFmt = "libsvm";

      // Cluster center number
      int centerNum = 3;
      // Feature number of train data
      int featureNum = 4;
      // Total iteration number
      int epochNum = 5;
      // Sample ratio per mini-batch
      double spratio = 1.0;
      // C
      double c = 0.15;

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

      //set Kmeans algorithm parameters #cluster #feature #epoch
      conf.set(MLConf.KMEANS_CENTER_NUM(), String.valueOf(centerNum));
      conf.set(MLConf.ML_FEATURE_INDEX_RANGE(), String.valueOf(featureNum));
      conf.set(MLConf.ML_EPOCH_NUM(), String.valueOf(epochNum));
      conf.set(MLConf.KMEANS_SAMPLE_RATIO_PERBATCH(), String.valueOf(spratio));
      conf.set(MLConf.kMEANS_C(), String.valueOf(c));

      // Set data format
      conf.set(MLConf.ML_DATA_INPUT_FORMAT(), dataFmt);
    } catch (Exception x) {
      LOG.error("setup failed ", x);
      throw x;
    }
  }

  @Test public void testKMeans() throws Exception {
    trainOnLocalClusterTest();
    predictOnLocalClusterTest();
  }

  private void trainOnLocalClusterTest() throws Exception {
    try {
      // Set trainning data path
      conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, inputPath);
      // Set save model path
      conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model");
      // Set log sava path
      conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/kmeansLog/log");
      // Set actionType train
      conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_TRAIN());

      KMeansRunner runner = new KMeansRunner();
      runner.train(conf);
    } catch (Exception x) {
      LOG.error("run trainOnLocalClusterTest failed ", x);
      throw x;
    }
  }

  private void predictOnLocalClusterTest() throws Exception {
    try {
      // Set trainning data path
      conf.set(AngelConf.ANGEL_PREDICT_DATA_PATH, inputPath);
      // Set load model path
      conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model");
      // Set predict result path
      conf.set(AngelConf.ANGEL_PREDICT_PATH, LOCAL_FS + TMP_PATH + "/predict");
      // Set actionType prediction
      conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_PREDICT());

      KMeansRunner runner = new KMeansRunner();
      runner.predict(conf);
    } catch (Exception x) {
      LOG.error("run predictOnLocalClusterTest failed ", x);
      throw x;
    }
  }
}
