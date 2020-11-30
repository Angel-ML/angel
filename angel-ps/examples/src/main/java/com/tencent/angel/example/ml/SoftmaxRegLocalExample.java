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
import com.tencent.angel.ml.core.conf.MLConf;
import com.tencent.angel.ml.core.graphsubmit.GraphRunner;
import com.tencent.angel.ml.matrix.RowType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.util.Scanner;

public class SoftmaxRegLocalExample {
  private static final Log LOG = LogFactory.getLog(SoftmaxRegLocalExample.class);

  private Configuration conf = new Configuration();

  private static boolean inPackage = false;
  private static String CLASSBASE = "com.tencent.angel.ml.classification.";

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
	  predictInput = "../data/usps/usps_256d_train.libsvm";
	} else {
	  trainInput = "data/usps/usps_256d_train.libsvm";
	  predictInput = "data/usps/usps_256d_train.libsvm";
	}

	// Data format, libsvm or dummy
	String dataType = "libsvm";
	// class number
	int classNum = 10;
	// Model type
	String modelType = String.valueOf(RowType.T_FLOAT_SPARSE);

	// Feature number of train data
	int featureNum = 256;
	// Total iteration number
	int epochNum = 5;
	// Validation sample Ratio
	double vRatio = 0.3;

	// Train batch number per epoch.
	double spRatio = 1;

	// Learning rate
	double learnRate = 0.5;
	// Decay of learning rate
	double decay = 1;
	// Regularization coefficient
	double reg = 0.002;

	// Set file system
	String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
	String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");

	// Set basic configuration keys
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
	  conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model/SoftmaxReg");
	} else if (mode == 2) { // incTrain mode
	  conf.set(AngelConf.ANGEL_ACTION_TYPE, "inctrain");
	  conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, trainInput);
	  conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model/SoftmaxReg");
	  conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model/SoftmaxReg-inc");
	} else if (mode == 3) {  // predict mode
	  conf.set(AngelConf.ANGEL_ACTION_TYPE, "predict");
	  conf.set(AngelConf.ANGEL_PREDICT_DATA_PATH, predictInput);
	  conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model/SoftmaxReg");
	  conf.set(AngelConf.ANGEL_PREDICT_PATH, LOCAL_FS + TMP_PATH + "/predict/SoftmaxReg");
	}
	conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/log");

	// Set angel resource parameters #worker, #task, #PS
	conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1);
	conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1);
	conf.setInt(AngelConf.ANGEL_PS_NUMBER, 1);

	// Set Softmax algorithm parameters
	conf.set(MLConf.ML_MODEL_TYPE(), modelType);
	conf.setInt(MLConf.ML_NUM_CLASS(), classNum);
	conf.set(MLConf.ML_DATA_LABEL_TRANS(), "SubOneTrans");
	conf.set(MLConf.ML_FEATURE_INDEX_RANGE(), String.valueOf(featureNum));
	conf.set(MLConf.ML_EPOCH_NUM(), String.valueOf(epochNum));
	conf.set(MLConf.ML_BATCH_SAMPLE_RATIO(), String.valueOf(spRatio));
	conf.set(MLConf.ML_VALIDATE_RATIO(), String.valueOf(vRatio));
	conf.set(MLConf.ML_LEARN_RATE(), String.valueOf(learnRate));
	conf.set(MLConf.ML_OPT_DECAY_ALPHA(), String.valueOf(decay));
	conf.set(MLConf.ML_REG_L2(), String.valueOf(reg));
	conf.setLong(MLConf.ML_MODEL_SIZE(), 124L);

	// Set model class
	conf.set(MLConf.ML_MODEL_CLASS_NAME(), CLASSBASE + "SoftmaxRegression");

  }

  public void train() {

	try {
	  setConf(1);

	  GraphRunner runner = new GraphRunner();
	  runner.train(conf);
	} catch (Exception e) {
	  LOG.error("run SoftmaxRegLocalExample:train failed.", e);
	  throw e;
	}

  }


  public void incTrain() {

	try {
	  setConf(2);

	  GraphRunner runner = new GraphRunner();
	  runner.train(conf);
	} catch (Exception e) {
	  LOG.error("run SoftmaxRegLocalExample:incTrain failed.", e);
	  throw e;
	}

  }


  public void predict() {

	try {
	  setConf(3);

	  GraphRunner runner = new GraphRunner();
	  runner.predict(conf);
	} catch (Exception e) {
	  LOG.error("run SoftmaxRegLocalExample:predict failed.", e);
	  throw e;
	}
  }

  public static void main(String[] args) throws Exception {
	SoftmaxRegLocalExample example = new SoftmaxRegLocalExample();
	Scanner scanner = new Scanner(System.in);
	System.out.println("1-train 2-incTrain 3-predict");
	System.out.println("Please input the mode:");
	int mode = scanner.nextInt();
	switch (mode) {
	  case 1:
		example.train();
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
