package com.tencent.angel.ml.classification.mlr

import com.tencent.angel.ml.MLRunner
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

/**
  * Created by hbghh on 2017/8/17.
  */
class MLRRunner extends MLRunner {
  private val LOG = LogFactory.getLog(classOf[MLRRunner])

  /**
    * Run LR train task
    *
    * @param conf : configuration of algorithm and resource
    */
  override
  def train(conf: Configuration): Unit = {
    conf.setInt("angel.worker.matrixtransfer.request.timeout.ms", 60000)

    train(conf, MLRModel(conf), classOf[MLRTrainTask])
  }

  /*
   * Run LR predict task
   * @param conf: configuration of algorithm and resource
   */
  override
  def predict(conf: Configuration): Unit = {
    conf.setInt("angel.worker.matrix.transfer.request.timeout.ms", 60000)
    super.predict(conf, MLRModel(conf), classOf[MLRPredictTask])
  }

  /*
   * Run LR incremental train task
   * @param conf: configuration of algorithm and resource
   */
  def incTrain(conf: Configuration): Unit = {
    conf.setInt("angel.worker.matrix.transfer.request.timeout.ms", 60000)
    super.train(conf, MLRModel(conf), classOf[MLRTrainTask])
  }
}

