package com.tencent.angel.ml.classification.lr

import com.tencent.angel.ml.MLRunner
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

/**
  * Run logistic regression task on angel with fixed-point compression
  */

class ZipLR extends MLRunner {
  private val LOG = LogFactory.getLog(classOf[LRRunner])

  /**
    * Run LR train task
    *
    * @param conf : configuration of algorithm and resource
    */
  override
  def train(conf: Configuration): Unit = {
    conf.setInt("angel.worker.matrixtransfer.request.timeout.ms", 60000)
    train(conf, LRModel(conf), classOf[LRTrainTask])
  }

  /*
   * Run LR predict task
   * @param conf: configuration of algorithm and resource
   */
  override
  def predict(conf: Configuration): Unit = {
    conf.setInt("angel.worker.matrix.transfer.request.timeout.ms", 60000)
    super.predict(conf, LRModel(conf), classOf[LRPredictTask])
  }

  /*
   * Run LR incremental train task
   * @param conf: configuration of algorithm and resource
   */
  def incTrain(conf: Configuration): Unit = {
    conf.setInt("angel.worker.matrix.transfer.request.timeout.ms", 60000)
    super.train(conf, LRModel(conf), classOf[LRTrainTask])
  }
}

