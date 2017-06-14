package com.tencent.angel.example.quickStart;

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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;

import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.ml.conf.MLConf;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.log4j.PropertyConfigurator;


/**
 * Gradient descent LR UT.
 */
public class myLRLocalCluster {

  static {
    PropertyConfigurator.configure("./angel-ps/conf/log4j.properties");
  }


  public static Configuration setConf() {
    Configuration conf = new Configuration();

    // Feature number of train data
    int featureNum = 124;
    // Total iteration number
    int epochNum = 20;
    // Learning rate
    double learnRate = 1.0;
    // Regularization coefficient
    double reg = 0.2;

    String inputPath = "./angel-ps/mllib/src/test/data/lr/a9a.train";
    String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
    String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
    String savePath = LOCAL_FS + TMP_PATH + "/model";

    // Set local deploy mode
    conf.set(AngelConfiguration.ANGEL_DEPLOY_MODE, "LOCAL");

    // Set basic configuration keys
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.set(AngelConfiguration.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
    conf.setBoolean(AngelConfiguration.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);

    //set angel resource parameters #worker, #task, #PS
    conf.setInt(AngelConfiguration.ANGEL_WORKERGROUP_NUMBER, 1);
    conf.setInt(AngelConfiguration.ANGEL_WORKER_TASK_NUMBER, 1);
    conf.setInt(AngelConfiguration.ANGEL_PS_NUMBER, 1);

    //set sgd LR algorithm parameters #feature #epoch
    conf.set(MLConf.ML_FEATURE_NUM(), String.valueOf(featureNum));
    conf.set(MLConf.ML_EPOCH_NUM(), String.valueOf(epochNum));
    conf.set(MLConf.ML_LEARN_RATE(), String.valueOf(learnRate));
    conf.set(MLConf.ML_REG_L2(), String.valueOf(reg));

    // Set input data path
    conf.set(AngelConfiguration.ANGEL_TRAIN_DATA_PATH, inputPath);
    // Set save model path
    conf.set(AngelConfiguration.ANGEL_SAVE_MODEL_PATH, savePath);
    // Set actionType train
    conf.set(AngelConfiguration.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_TRAIN());

    return conf;
  }

  public static void main(String[] args) {
    Configuration conf = setConf();

    myLRRunner runner = new myLRRunner();

    runner.train(conf);
  }

}
