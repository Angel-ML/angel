package com.tencent.angel.ml.algorithm;

import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.ml.algorithm.classification.lr.LRRunner;
import com.tencent.angel.ml.algorithm.conf.MLConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

/**
 * Gradient descent LR UT.
 */
public class SgdLRTest {
  private Configuration conf = new Configuration();

  static {
    PropertyConfigurator.configure("../log4j.properties");
  }

  /**
   * set parameter values of conf
   */
  @Before
  public void setConf() {

    // Feature number of train data
    int featureNum = 124;
    // Total iteration number
    int epochNum = 20;
    // Validation sample Ratio
    double vRatio = 0.1;
    // Data format, libsvm or dummy
    String dataFmt = "libsvm";
    // Train batch number per epoch.
    int batchNum = 10;

    // Learning rate
    double learnRate = 1;
    // Decay of learning rate
    double decay = 0.1;
    // Regularization coefficient
    double reg = 0;

    // Set local deploy mode
    conf.set(AngelConfiguration.ANGEL_DEPLOY_MODE, "LOCAL");

    // Set basic configuration keys
    conf.setBoolean("mapred.mapper.new-api", true);
    conf.set(AngelConfiguration.ANGEL_INPUTFORMAT_CLASS, CombineTextInputFormat.class.getName());
    conf.setBoolean(AngelConfiguration.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, true);

    // Set data format
    conf.set(MLConf.ML_DATAFORMAT(), dataFmt);

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
    String inputPath = "./src/test/data/lr/a9a.train";
    String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
    String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");

    // Set trainning data path
    conf.set(AngelConfiguration.ANGEL_TRAIN_DATA_PATH, inputPath);
    // Set save model path
    conf.set(AngelConfiguration.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model");
    // Set actionType train
    conf.set(MLConf.ANGEL_ACTION_TYPE(), MLConf.ANGEL_ML_TRAIN());

    LRRunner runner = new LRRunner();
    runner.train(conf);
 }


  @Test
  public void incTrainTest() {
    String inputPath = "./src/test/data/lr/a9a.train";
    String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
    String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");

    // Set trainning data path
    conf.set(AngelConfiguration.ANGEL_TRAIN_DATA_PATH, inputPath);
    // Set load model path
    conf.set(AngelConfiguration.ANGEL_LOAD_MODEL_PATH, LOCAL_FS+TMP_PATH+"/model");
    // Set save model path
    conf.set(AngelConfiguration.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/newmodel");
    // Set actionType incremental train
    conf.set(MLConf.ANGEL_ACTION_TYPE(), MLConf.ANGEL_ML_INC_TRAIN());

    LRRunner runner = new LRRunner();
    runner.incTrain(conf);
  }

  @Test
  public void predictTest() {
    String inputPath = "./src/test/data/lr/a9a.test";
    String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
    String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");

    // Set trainning data path
    conf.set(AngelConfiguration.ANGEL_TRAIN_DATA_PATH, inputPath);
    // Set load model path
    conf.set(AngelConfiguration.ANGEL_LOAD_MODEL_PATH, LOCAL_FS+TMP_PATH+"/model");
    // Set predict result path
    conf.set(AngelConfiguration.ANGEL_PREDICT_PATH, LOCAL_FS + TMP_PATH + "/predict");
    // Set actionType prediction
    conf.set(MLConf.ANGEL_ACTION_TYPE(), MLConf.ANGEL_ML_INC_TRAIN());

    conf.set(MLConf.ANGEL_ACTION_TYPE(), MLConf.ANGEL_ML_PREDICT());
    LRRunner runner = new LRRunner();

    runner.predict(conf);
  }
}
