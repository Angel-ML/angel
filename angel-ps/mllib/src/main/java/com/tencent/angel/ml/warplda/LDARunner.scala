package com.tencent.angel.ml.warplda

import com.tencent.angel.client.AngelClientFactory
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.data.inputformat.BalanceInputFormat
import com.tencent.angel.ml.MLRunner
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

/**
  * Created by chris on 9/1/17.
  */
class LDARunner extends MLRunner {

  val LOG = LogFactory.getLog(classOf[LDARunner])

  /**
    * Training job to obtain a model
    */
  override
  def train(conf: Configuration): Unit = {
    conf.setInt(AngelConf.ANGEL_WORKER_MAX_ATTEMPTS, 1)
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1)
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, classOf[BalanceInputFormat].getName)
    LOG.info(s"n_tasks=${conf.getInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 0)}")
    //    train(conf, new LDAModel(conf), classOf[LDATrainTask])

    val client = AngelClientFactory.get(conf)

    client.startPSServer()
    client.loadModel(new LDAModel(conf))
    client.runTask(classOf[LDATrainTask])
    client.waitForCompletion()
    //    client.saveModel(model)

    client.stop()
  }

  /**
    * Using a model to predict with unobserved samples
    */
  override def predict(conf: Configuration): Unit = {
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1)
    conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, classOf[BalanceInputFormat].getName)
    val client = AngelClientFactory.get(conf)

    client.startPSServer()
    client.loadModel(new LDAModel(conf))
    client.runTask(classOf[LDAInferTask])
    client.waitForCompletion()
    //    client.saveModel(model)

    client.stop()
  }

  /**
    * Incremental training job to obtain a model based on a trained model
    */
  override def incTrain(conf: Configuration): Unit = ???
}
