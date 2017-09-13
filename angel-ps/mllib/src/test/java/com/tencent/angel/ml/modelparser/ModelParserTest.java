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

package com.tencent.angel.ml.modelparser;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.conf.MLConf;
import com.tencent.angel.ml.lr.SgdLRTest;
import com.tencent.angel.ml.toolkits.modelconverter.ModelConverterRunner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;

public class ModelParserTest {
  private Configuration conf = new Configuration();
  private static final Log LOG = LogFactory.getLog(SgdLRTest.class);
  private static String LOCAL_FS = FileSystem.DEFAULT_FS;
  private static String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
  int threadNumber = 1;

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  public void setup() {
    // Set local deploy mode
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");

    // Set basic configuration keys
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);

    // Set actionType train
    conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_TRAIN());

    // Set local deploy mode
    conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL");

    // Set basic configuration keys
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
    conf.setBoolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);


    //set angel resource parameters #worker, #task, #PS
    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1);
    conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);
  }


  @Test
  public void ModelParser() throws Exception {
    parseDenseDouble();
    //parseSparseDouble();
    //parseDenseFloat();
    //parseDenseInt();
    //parseSparseInt();
  }

  public void parseDenseDouble() {
    setup();

    String modelInPath  = "./src/test/data/model/";
    String modelName = "DenseDouble";
    String modelOutPath = LOCAL_FS + TMP_PATH + "/modelParser/" + modelName;
    String psOutPath = LOCAL_FS + TMP_PATH + "/parsedModel";

    // Set model path
    conf.set(MLConf.ML_MODEL_IN_PATH(), modelInPath);
    conf.set(MLConf.ML_MODEL_NAME(), modelName);
    conf.set(MLConf.ML_MODEL_OUT_PATH(), modelOutPath);
    conf.setInt(MLConf.ML_MODEL_CONVERT_THREAD_COUNT(), threadNumber);
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, psOutPath);

    ModelConverterRunner runner = new ModelConverterRunner();
    runner.submit(conf);
  }

  public void parseSparseDouble() {
    setup();

    String modelInPath  = "./src/test/data/model/";
    String modelName = "SparseDouble";
    String modelOutPath = LOCAL_FS + TMP_PATH + "/modelParser/" + modelName;
    String psOutPath = LOCAL_FS + TMP_PATH + "/parsedModel";

    // Set model path
    conf.set(MLConf.ML_MODEL_IN_PATH(), modelInPath);
    conf.set(MLConf.ML_MODEL_NAME(), modelName);
    conf.set(MLConf.ML_MODEL_OUT_PATH(), modelOutPath);
    conf.setInt(MLConf.ML_MODEL_CONVERT_THREAD_COUNT(), threadNumber);
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, psOutPath);

    ModelConverterRunner runner = new ModelConverterRunner();
    runner.submit(conf);
  }

  public void parseDenseFloat() {
    setup();

    String modelInPath  = "./src/test/data/model/";
    String modelName = "DenseFloat";
    String modelOutPath = LOCAL_FS + TMP_PATH + "/modelParser/" + modelName;
    String psOutPath = LOCAL_FS + TMP_PATH + "/parsedModel";

    // Set model path
    conf.set(MLConf.ML_MODEL_IN_PATH(), modelInPath);
    conf.set(MLConf.ML_MODEL_NAME(), modelName);
    conf.set(MLConf.ML_MODEL_OUT_PATH(), modelOutPath);
    conf.setInt(MLConf.ML_MODEL_CONVERT_THREAD_COUNT(), threadNumber);
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, psOutPath);

    ModelConverterRunner runner = new ModelConverterRunner();
    runner.submit(conf);
  }

  public void parseDenseInt() {
    setup();

    String modelInPath  = "./src/test/data/model/";
    String modelName = "DenseInt";
    String modelOutPath = LOCAL_FS + TMP_PATH + "/modelParser/" + modelName;
    String psOutPath = LOCAL_FS + TMP_PATH + "/parsedModel";

    // Set model path
    conf.set(MLConf.ML_MODEL_IN_PATH(), modelInPath);
    conf.set(MLConf.ML_MODEL_NAME(), modelName);
    conf.set(MLConf.ML_MODEL_OUT_PATH(), modelOutPath);
    conf.setInt(MLConf.ML_MODEL_CONVERT_THREAD_COUNT(), threadNumber);
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, psOutPath);

    ModelConverterRunner runner = new ModelConverterRunner();
    runner.submit(conf);
  }

  public void parseSparseInt() {
    setup();

    String modelInPath  = "./src/test/data/model/";
    String modelName = "SparseInt";
    String modelOutPath = LOCAL_FS + TMP_PATH + "/modelParser/" + modelName;
    String psOutPath = LOCAL_FS + TMP_PATH + "/parsedModel";

    // Set model path
    conf.set(MLConf.ML_MODEL_IN_PATH(), modelInPath);
    conf.set(MLConf.ML_MODEL_NAME(), modelName);
    conf.set(MLConf.ML_MODEL_OUT_PATH(), modelOutPath);
    conf.setInt(MLConf.ML_MODEL_CONVERT_THREAD_COUNT(), threadNumber);
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, psOutPath);

    ModelConverterRunner runner = new ModelConverterRunner();
    runner.submit(conf);
  }
}
