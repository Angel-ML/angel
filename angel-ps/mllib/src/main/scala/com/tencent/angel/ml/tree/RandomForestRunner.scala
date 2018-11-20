package com.tencent.angel.ml.tree

import com.tencent.angel.client.AngelClientFactory
import com.tencent.angel.ml.core.MLRunner
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration
import com.tencent.angel.ml.tree.model.RandomForestModel

class RandomForestRunner extends MLRunner {

  val LOG = LogFactory.getLog(classOf[RandomForestRunner])

  override def train(conf: Configuration): Unit = {

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
