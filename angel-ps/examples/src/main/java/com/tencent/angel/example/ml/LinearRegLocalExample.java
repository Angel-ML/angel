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
 */
package com.tencent.angel.example.ml;


import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.conf.MLConf;
import com.tencent.angel.ml.regression.linear.LinearRegRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;

import java.util.Scanner;

public class LinearRegLocalExample {
  private Configuration conf = new Configuration();
  private static final Log LOG = LogFactory.getLog(LinearRegLocalExample.class);
  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  public void setConf() {

    // Feature number of train data
    int featureNum = 101;
    // Total iteration number
    int epochNum = 20;
    // Validation sample Ratio
    double vRatio = 0.5;
    // Data format, libsvm or dummy
    String dataFmt = "libsvm";
    // Train batch number per epoch.
    double spRatio = 1;

    // Learning rate
    double learnRate = 0.1;
    // Decay of learning rate
    double decay = 0.01;
    // Regularization coefficient
    double reg = 0;

    // Set local deploy mode
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");

    // Set basic configuration keys
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);

    // Set data format
    conf.set(MLConf.ML_DATA_INPUT_FORMAT(), dataFmt);

    // set angel resource parameters #worker, #task, #PS
    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);

    // set sgd LR algorithm parameters #feature #epoch
    conf.set(MLConf.ML_FEATURE_INDEX_RANGE(), String.valueOf(featureNum));
    conf.set(MLConf.ML_EPOCH_NUM(), String.valueOf(epochNum));
    conf.set(MLConf.ML_BATCH_SAMPLE_RATIO(), String.valueOf(spRatio));
    conf.set(MLConf.ML_VALIDATE_RATIO(), String.valueOf(vRatio));
    conf.set(MLConf.ML_LEARN_RATE(), String.valueOf(learnRate));
    conf.set(MLConf.ML_LEARN_DECAY(), String.valueOf(decay));
    conf.set(MLConf.ML_LR_REG_L2(), String.valueOf(reg));
  }

  public void trainOnLocalCluster() throws Exception {
    setConf();
    String inputPath = "../data/exampledata/LinearRegression";
    String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
    String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
    String logPath = "./src/test/log";

    // Set trainning data path
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, inputPath);
    // Set save model path
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model");
    // Set log path
    conf.set(AngelConf.ANGEL_LOG_PATH, logPath);
    // Set actionType train
    conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_TRAIN());


    LinearRegRunner runner = new LinearRegRunner();
    runner.train(conf);
  }


  public void incTrain() {
    setConf();
    String inputPath = "../data/exampledata/LinearRegression/LinearReg100.train";
    String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
    String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
    String logPath = "./src/test/log";

    // Set trainning data path
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, inputPath);
    // Set load model path
    conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model");
    // Set save model path
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/newmodel");
    // Set actionType incremental train
    conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_INC_TRAIN());
    // Set log path
    conf.set(AngelConf.ANGEL_LOG_PATH, logPath);

    LinearRegRunner runner = new LinearRegRunner();
    runner.incTrain(conf);
  }

  public void predict() {
    setConf();
    String inputPath = "../data/exampledata/LinearRegression/LinearReg100.train";
    String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
    String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");

    // Set trainning data path
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, inputPath);
    // Set load model path
    conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model");
    // Set predict result path
    conf.set(AngelConf.ANGEL_PREDICT_PATH, LOCAL_FS + TMP_PATH + "/predict");
    // Set actionType prediction
    conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_PREDICT());
    LinearRegRunner runner = new LinearRegRunner();

    runner.predict(conf);
  }

  public static void main(String[] args) throws Exception {
    System.out.println(System.getProperty("java.class.path"));
    LinearRegLocalExample example = new LinearRegLocalExample();
    Scanner scanner = new Scanner(System.in);
    System.out.println("1-trainOnLocalCluster 2-incTrain 3-predict");
    System.out.println("Please input the mode:");
    int mode = scanner.nextInt();
    switch (mode) {
      case 1:
        example.trainOnLocalCluster();
        break;
      case 2:
        example.incTrain();
        break;
      case 3:
        example.predict();
        break;
    }

    System.exit(0);
  }


}
