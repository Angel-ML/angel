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
import com.tencent.angel.ml.conf.MLConf;
import com.tencent.angel.ml.matrixfactorization.MatrixFactorizationRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;

import java.util.Scanner;


public class MFLocalExample {
  private static final Log LOG = LogFactory.getLog(MFLocalExample.class);
  private Configuration conf = new Configuration();

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }


  public void setConf() throws Exception {
    String inputPath = "../data/exampledata/MFLocalExampleData";
    // Set local deploy mode
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");
    // Set basic configuration keys
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);

    // set angel resource parameters #worker, #task, #PS
    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);

    String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
    String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");

    // Set trainning data, and save model path
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, inputPath);
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model");
    conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/log");
    // Set actionType train
    conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_TRAIN());

    // Set MF algorithm parameters
    conf.set(MLConf.ML_MF_RANK(), "200");
    conf.set(MLConf.ML_EPOCH_NUM(), "8");
    conf.set(MLConf.ML_MF_ROW_BATCH_NUM(), "2");
    conf.set(MLConf.ML_MF_ITEM_NUM(), "1683");
    conf.set(MLConf.ML_MF_LAMBDA(), "0.01");
    conf.set(MLConf.ML_MF_ETA(), "0.0054");
  }


  public void train() throws Exception {
    setConf();
    MatrixFactorizationRunner runner = new MatrixFactorizationRunner();
    runner.train(conf);
  }

  public static void main(String[] args) throws Exception {
    System.out.println(System.getProperty("user.dir"));
    MFLocalExample example = new MFLocalExample();
    Scanner scanner = new Scanner(System.in);
    System.out.println("1-train");
    System.out.println("Please input the mode:");
    int mode = scanner.nextInt();
    switch (mode) {
      case 1:
        example.train();
        break;
      case 2:
        break;
    }

    System.exit(0);
  }

}
