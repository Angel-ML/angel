package com.tencent.angel.ml.treemodels.gbdt

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.exception.AngelException
import com.tencent.angel.ml.MLRunner
import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.treemodels.gbdt.dp.DPGBDTModel
import com.tencent.angel.ml.treemodels.gbdt.fp.FPGBDTModel
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.conf.Configuration

class GBDTRunner extends MLRunner {
  private val LOG: Log = LogFactory.getLog(classOf[GBDTRunner])

  /**
    * Training job to obtain a model
    */
  override def train(conf: Configuration): Unit = {
    var mem = conf.getInt(AngelConf.ANGEL_WORKER_MEMORY_GB, 1) * 1000
    var javaOpts = s"-Xmx${mem}M -Xms${mem}M -XX:+UseConcMarkSweepGC -XX:+PrintGCTimeStamps -XX:+PrintGCDetails"
    LOG.info(javaOpts)
    conf.set(AngelConf.ANGEL_WORKER_JAVA_OPTS, javaOpts)

    mem = conf.getInt(AngelConf.ANGEL_PS_MEMORY_GB, 1) * 1000
    javaOpts = s"-Xmx${mem}M -Xms${mem}M -XX:+UseConcMarkSweepGC -XX:+PrintGCTimeStamps -XX:+PrintGCDetails"
    conf.set(AngelConf.ANGEL_PS_JAVA_OPTS, javaOpts)
    LOG.info(javaOpts)


    var numFeature = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
    val numPS = conf.getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER)

    if (numFeature % numPS != 0) {
      numFeature = (numFeature / numPS + 1) * numPS
      conf.setInt(MLConf.ML_FEATURE_NUM, numFeature)
      LOG.info(s"PS num: $numPS, true feature num: $numFeature")
    }

    val parallelMode = conf.get(MLConf.ML_GBDT_PARALLEL_MODE, MLConf.DEFAULT_ML_GBDT_PARALLEL_MODE)
    parallelMode match {
      case ParallelMode.DATA_PARALLEL => train(conf, DPGBDTModel(conf), classOf[GBDTTrainTask])
      case ParallelMode.FEATURE_PARALLEL => train(conf, FPGBDTModel(conf), classOf[GBDTTrainTask])
      case _ => throw new AngelException("No such parallel mode: " + parallelMode)
    }
  }

  /**
    * Incremental training job to obtain a model based on a trained model
    */
  override def incTrain(conf: Configuration): Unit = ???

  /**
    * Using a model to predict with unobserved samples
    */
  override def predict(conf: Configuration): Unit = {
    conf.setInt("angel.worker.matrix.transfer.request.timeout.ms", 60000)
    super.predict(conf, FPGBDTModel(conf), classOf[GBDTPredictTask])
  }
}
