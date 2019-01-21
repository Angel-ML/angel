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
import com.tencent.angel.ml.clustering.kmeans.KMeansRunner;
import com.tencent.angel.ml.core.conf.MLConf;
import com.tencent.angel.ml.matrix.RowType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.util.Scanner;

public class KMeansLocalExample {

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
      trainInput = "../data/usps/usps_256d_train.libsvm";
      predictInput = "../data/usps/usps_256d_test.libsvm";
    } else {
      trainInput = "data/usps/usps_256d_train.libsvm";
      predictInput = "data/usps/usps_256d_test.libsvm";
    }

    // Data format
    String dataType = "libsvm";
    // Model type
    String modelType = String.valueOf(RowType.T_DOUBLE_SPARSE);

    // Cluster center number
    int centerNum = 10;
    // Feature number of train data
    long featureNum = 256;
    // Total iteration number
    int epochNum = 50;
    // Sample ratio per mini-batch
    double spratio = 1.0;
    // C
    double c = 0.5;

    // Set file system
    String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
    String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");

    conf.setBoolean("mapred.mapper.new-api", true);
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);
    conf.setInt(AngelConf.ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS, 50);

    // Use local deploy mode and data format
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");
    conf.set(MLConf.ML_DATA_INPUT_FORMAT(), String.valueOf(dataType));

    // Set data path
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
    if (mode == 1) {  // train mode
      conf.set(AngelConf.ANGEL_ACTION_TYPE, "train");
      conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, trainInput);
      conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model/kmeans");
    } else if (mode == 2) {  // predict mode
      conf.set(AngelConf.ANGEL_ACTION_TYPE, "inctrain");
      conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, trainInput);
      conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model/kmeans");
      conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model/kmeans-inc");
    } else if (mode == 3) {  // predict mode
      conf.set(AngelConf.ANGEL_ACTION_TYPE, "predict");
      conf.set(AngelConf.ANGEL_PREDICT_DATA_PATH, predictInput);
      conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model/kmeans");
      conf.set(AngelConf.ANGEL_PREDICT_PATH, LOCAL_FS + TMP_PATH + "/predict/kmeans");
    }
    conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/log");

    // Set angel resource, #worker, #task, #PS
    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);

    //set Kmeans algorithm parameters #cluster #feature #epoch
    conf.set(MLConf.ML_MODEL_TYPE(), modelType);
    conf.set(MLConf.KMEANS_CENTER_NUM(), String.valueOf(centerNum));
    conf.set(MLConf.ML_FEATURE_INDEX_RANGE(), String.valueOf(featureNum));
    conf.set(MLConf.ML_EPOCH_NUM(), String.valueOf(epochNum));
    conf.set(MLConf.KMEANS_C(), String.valueOf(c));

  }

  public void train() {

    try {
      setConf(1);

      KMeansRunner runner = new KMeansRunner();
      runner.train(conf);
    } catch (Exception e) {
      LOG.error("run KMeansLocalExample:train failed.", e);
      throw e;
    }

  }

  public void inctrain() {

    try {
      setConf(2);

      KMeansRunner runner = new KMeansRunner();
      runner.train(conf);
    } catch (Exception e) {
      LOG.error("run KMeansLocalExample:train failed.", e);
      throw e;
    }

  }

  public void predict() {

    try {
      setConf(3);

      KMeansRunner runner = new KMeansRunner();
      runner.predict(conf);
    } catch (Exception e) {
      LOG.error("run KMeansLocalExample:predict failed.", e);
      throw e;
    }
  }

  public static void main(String[] args) {
    KMeansLocalExample example = new KMeansLocalExample();
    Scanner scanner = new Scanner(System.in);
    System.out.println("1-train 2-inctrain 3-predict");
    System.out.println("Please input the mode:");
    int mode = scanner.nextInt();
    switch (mode) {
      case 1:
        example.train();
        break;
      case 2:
        example.inctrain();
        break;
      case 3:
        example.predict();
        break;
    }

    System.exit(0);
  }

}
