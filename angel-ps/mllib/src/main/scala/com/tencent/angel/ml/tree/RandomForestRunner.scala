package com.tencent.angel.ml.tree

import com.tencent.angel.client.AngelClientFactory
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.core.MLRunner
import com.tencent.angel.ml.core.conf.MLConf
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import com.tencent.angel.ml.tree.model.RandomForestModel

class RandomForestRunner extends MLRunner {

  val LOG = LogFactory.getLog(classOf[RandomForestRunner])

  override def train(conf: Configuration): Unit = {

    val numTasks = conf.getInt(AngelConf.ANGEL_WORKERGROUP_NUMBER,
      AngelConf.DEFAULT_ANGEL_WORKERGROUP_NUMBER) *
      conf.getInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, AngelConf.DEFAULT_ANGEL_WORKER_TASK_NUMBER)
    val numTrees = conf.getInt(MLConf.ML_NUM_TREE, MLConf.DEFAULT_ML_NUM_TREE)
    if (numTrees < numTasks) {
      conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, numTrees)
      conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1)
    }

    val client = AngelClientFactory.get(conf)
    val model = RandomForestModel(conf)

    try {
      client.startPSServer()
      client.loadModel(model)
      client.runTask(classOf[RandomForestTrainTask])
      client.waitForCompletion()
      client.saveModel(model)
    } finally {
      client.stop()
    }
  }

  override def predict(conf: Configuration) {
    conf.setInt("angel.worker.matrix.transfer.request.timeout.ms", 60000)

    val client = AngelClientFactory.get(conf)
    val model = RandomForestModel(conf)

    try {
      client.startPSServer()
      client.loadModel(model)
      client.runTask(classOf[RandomForestPredictTask])
      client.waitForCompletion()
    } finally {
      client.stop()
    }
  }

}
