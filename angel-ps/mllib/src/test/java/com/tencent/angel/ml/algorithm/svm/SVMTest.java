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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.algorithm.svm;

import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.ml.algorithm.classification.svm.SVMRunner;
import com.tencent.angel.ml.algorithm.conf.MLConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

@RunWith(MockitoJUnitRunner.class)
public class SVMTest {

  private static final Log LOG = LogFactory.getLog(SVMTest.class);

  private Configuration conf = new Configuration();

  static {
    PropertyConfigurator.configure("../log4j.properties");
  }

  @BeforeClass
  public void setup() {
    String inputPath = "./src/test/data/svm/";

    // Feature number of train data
    int featureNum = 21000;
    // Total iteration number
    int epochNum = 2;
    // Validation Ratio
    double vRatio = 0.1;
    // Data format
    String dataFmt = "libsvm";
    // Train batch number per epoch.
    int batchNum = 10;

    // Learning rate
    double learnRate = 0.01;
    // Decay of learning rate
    double decay = 0;
    // Regularization coefficient
    double reg = 0;

    // Set basic configuration keys
    String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
    String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");

    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean(AngelConfiguration.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);

    // Use local deploy mode and dummy data spliter
    conf.set(AngelConfiguration.ANGEL_DEPLOY_MODE, "LOCAL");
    
    // set input, output path
    conf.set(AngelConfiguration.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
    conf.set(AngelConfiguration.ANGEL_TRAIN_DATA_PATH, inputPath);
    conf.set(AngelConfiguration.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/out");

    //set angel resource parameters #worker, #task, #PS
    conf.setInt(AngelConfiguration.ANGEL_WORKERGROUP_NUMBER, 1);
    conf.setInt(AngelConfiguration.ANGEL_WORKER_TASK_NUMBER, 1);
    conf.setInt(AngelConfiguration.ANGEL_PS_NUMBER, 1);

    //set sgd LR algorithm parameters #feature #epoch
    conf.set(MLConf.ML_FEATURE_NUM(), String.valueOf(featureNum));
    conf.set(MLConf.ML_EPOCH_NUM(), String.valueOf(epochNum));
    conf.set(MLConf.ML_BATCH_NUM(), String.valueOf(batchNum));
    conf.set(MLConf.ML_VALIDATE_RATIO(), String.valueOf(vRatio));
    conf.set(MLConf.ML_LEAR_RATE(), String.valueOf(learnRate));
    conf.set(MLConf.ML_LEARN_DECAY(), String.valueOf(decay));
    conf.set(MLConf.ML_REG_L2(), String.valueOf(reg));
  }

  @Test
  public void trainOnLocalClusterTest() throws Exception {
    // Submit LR Train Task
    SVMRunner runner = new SVMRunner();
    runner.train(conf);

    AngelClient angelClient = AngelClientFactory.get(conf);
    angelClient.stop();
  }


  @Test
  public void incLearnTest()  throws IOException{
    // Load Model from HDFS.
    String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
    conf.set("ml.svm.w.path", TMP_PATH + "/out/w");

    SVMRunner runner = new SVMRunner();

    runner.train(conf);

    AngelClient angelClient = AngelClientFactory.get(conf);
    angelClient.stop();
  }
}
