package com.tencent.angel.ml.factorizationmachines;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.conf.MLConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;
import org.junit.Test;

public class FMTest {
  private Configuration conf = new Configuration();

  static {
    PropertyConfigurator.configure("../conf/log4j.properties");
  }

  /**
   * set parameter values of conf
   */
  @Before
  public void setConf() {

    // Feature number of train data
    int featureNum = 236;
    // Total iteration number
    int epochNum = 20;
    // Rank
    int rank = 5;
    // Regularization parameters
    double reg0 = 0.0;
    double reg1 = 0.0;
    double reg2 = 0.001;
    // Learn rage
    double lr = 0.001;
    double stev = 0.1;

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

    //set FM algorithm parameters #feature #epoch
    conf.set(MLConf.ML_FEATURE_NUM(), String.valueOf(featureNum));
    conf.set(MLConf.ML_EPOCH_NUM(), String.valueOf(epochNum));
    conf.set(MLConf.ML_FM_RANK(), String.valueOf(rank));
    conf.set(MLConf.ML_LEARN_RATE(), String.valueOf(lr));
    conf.set(MLConf.ML_FM_REG0(), String.valueOf(reg0));
    conf.set(MLConf.ML_FM_REG1(), String.valueOf(reg1));
    conf.set(MLConf.ML_FM_REG2(), String.valueOf(reg2));
    conf.set(MLConf.ML_FM_V_STDDEV(), String.valueOf(stev));
  }

  @Test
  public void trainOnLocalClusterTest() throws Exception {
    String inputPath = "./src/test/data/fm/food_fm_libsvm";
    String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
    String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
    String savePath = LOCAL_FS + TMP_PATH + "/model";
    String logPath = LOCAL_FS + TMP_PATH + "/LRlog";

    // Set trainning data path
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, inputPath);
    // Set save model path
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, savePath);
    // Set log path
    conf.set(AngelConf.ANGEL_LOG_PATH, logPath);
    // Set actionType train
    conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_TRAIN());

    FMRunner runner = new FMRunner();
    runner.train(conf);
  }

  @Test
  public void FMClassificationTest() throws Exception {
    String inputPath = "./src/test/data/fm/a9a.train";
    String LOCAL_FS = LocalFileSystem.DEFAULT_FS;
    String TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp");
    String savePath = LOCAL_FS + TMP_PATH + "/model";
    String logPath = LOCAL_FS + TMP_PATH + "/LRlog";

    // Set trainning data path
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, inputPath);
    // Set save model path
    conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, savePath);
    // Set log path
    conf.set(AngelConf.ANGEL_LOG_PATH, logPath);
    // Set actionType train
    conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_TRAIN());
    // Set learnType
    conf.set(MLConf.ML_FM_LEARN_TYPE(), "c");
    // Set feature number
    conf.set(MLConf.ML_FEATURE_NUM(), String.valueOf(124));

    FMRunner runner = new FMRunner();
    runner.train(conf);
  }
}

