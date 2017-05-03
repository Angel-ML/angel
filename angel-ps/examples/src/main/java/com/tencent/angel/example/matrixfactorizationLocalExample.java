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

package com.tencent.angel.example;

import com.tencent.angel.client.AngelClient;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.ml.algorithm.conf.MLConf;
import com.tencent.angel.ml.algorithm.matrixfactorization.MatrixFactorizationRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;

import java.util.Scanner;


public class matrixfactorizationLocalExample {
  private static final Log LOG = LogFactory.getLog(matrixfactorizationLocalExample.class);
  private AngelClient angelClient;

  private Configuration conf = new Configuration();


  static {
    PropertyConfigurator.configure("/Users/mac/Documents/newdevelop-1.1.8/angel/angel-ps/log4j.properties");
  }


  public void setConf() throws Exception {
    String inputPath = "/Users/mac/Documents/newdevelop-1.1.8/angel/angel-ps/mllib/src/test/data/MovieLensDataSet";

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

    String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
    String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");

    // Set trainning data, save model path
    conf.set(AngelConfiguration.ANGEL_TRAIN_DATA_PATH, inputPath);
    conf.set(AngelConfiguration.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model");
    // Set actionType train
    conf.set(MLConf.ANGEL_ACTION_TYPE(), MLConf.ANGEL_ML_TRAIN());

    // Set MF algorithm parameters
    conf.set(MLConf.ML_MF_RANK(), "200");
    conf.set(MLConf.ML_EPOCH_NUM(), "5");
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
    matrixfactorizationLocalExample example = new matrixfactorizationLocalExample();
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
  }

}
