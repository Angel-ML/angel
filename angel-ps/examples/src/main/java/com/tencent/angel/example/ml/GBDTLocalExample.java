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


package com.tencent.angel.example.ml;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.GBDT.GBDTRunner;
import com.tencent.angel.ml.core.conf.MLConf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.util.Scanner;


public class GBDTLocalExample {

  private static final Log LOG = LogFactory.getLog(GBDTLocalExample.class);

  private Configuration conf = new Configuration();

  private static boolean inPackage = false;

  static {
    File confFile = new File("../conf/log4j.properties");
    if (confFile.exists()) {
      PropertyConfigurator.configure("../conf/log4j.properties");
      inPackage = true;
    } else {
      PropertyConfigurator.configure("angel-ps/conf/log4j.properties");
    }
  }

  public void setConf(int mode) {
    String trainInput = "";
    String predictInput = "";

    // Dataset
    if (inPackage) {
      trainInput = "../data/agaricus/agaricus_127d_train.libsvm";
      predictInput = "../data/agaricus/agaricus_127d_test.libsvm";
    } else {
      trainInput = "data/agaricus/agaricus_127d_train.libsvm";
      predictInput = "data/agaricus/agaricus_127d_test.libsvm";
    }

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

    // Set file system
    String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
    String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");

    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
    conf.setInt(AngelConf.ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS, 50);

    // Use local deploy mode and data format
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");
    conf.set(MLConf.ML_DATA_INPUT_FORMAT(), String.valueOf(dataType));
    conf.set(MLConf.ML_MODEL_TYPE(), MLConf.DEFAULT_ML_MODEL_TYPE());

    // Set data path
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
    if (mode == 1) {  // train mode
      conf.set(AngelConf.ANGEL_ACTION_TYPE, "train");
      conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, trainInput);
      conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model/gbdt");
    } else if (mode == 2) {  // predict mode
      conf.set(AngelConf.ANGEL_ACTION_TYPE, "predict");
      conf.set(AngelConf.ANGEL_PREDICT_DATA_PATH, predictInput);
      conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model/gbdt");
      conf.set(AngelConf.ANGEL_PREDICT_PATH, LOCAL_FS + TMP_PATH + "/predict/gbdt");
    }
    conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/log");

    // Set angel resource, #worker, #task, #PS
    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);

    // Set GBDT algorithm parameters
    conf.set(MLConf.ML_FEATURE_INDEX_RANGE(), String.valueOf(featureNum));
    conf.set(MLConf.ML_GBDT_TREE_NUM(), String.valueOf(treeNum));
    conf.set(MLConf.ML_GBDT_TREE_DEPTH(), String.valueOf(treeDepth));
    conf.set(MLConf.ML_GBDT_SPLIT_NUM(), String.valueOf(splitNum));
    conf.set(MLConf.ML_VALIDATE_RATIO(), String.valueOf(validateRatio));
    conf.set(MLConf.ML_GBDT_SAMPLE_RATIO(), String.valueOf(sampleRatio));
    conf.set(MLConf.ML_LEARN_RATE(), String.valueOf(learnRate));

  }

  public void train() {

    try {
      setConf(1);

      GBDTRunner runner = new GBDTRunner();
      runner.train(conf);
    } catch (Exception e) {
      LOG.error("run GBDTLocalExample:train failed.", e);
      throw e;
    }

  }

  public void predict() {

    try {
      setConf(2);

      GBDTRunner runner = new GBDTRunner();
      runner.predict(conf);
    } catch (Exception e) {
      LOG.error("run GBDTLocalExample:predict failed.", e);
      throw e;
    }
  }

  public static void main(String[] args) {
    GBDTLocalExample example = new GBDTLocalExample();
    Scanner scanner = new Scanner(System.in);
    System.out.println("1-train 2-predict");
    System.out.println("Please input the mode:");
    int mode = scanner.nextInt();
    switch (mode) {
      case 1:
        example.train();
        break;
      case 2:
        example.predict();
        break;
    }

    System.exit(0);
  }
}
