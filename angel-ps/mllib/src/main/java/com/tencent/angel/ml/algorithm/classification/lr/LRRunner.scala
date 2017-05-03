package com.tencent.angel.ml.algorithm.classification.lr

import com.tencent.angel.client.AngelClientFactory
import com.tencent.angel.conf.AngelConfiguration
import com.tencent.angel.ml.algorithm.MLRunner
import org.apache.hadoop.conf.Configuration

/**
  * Run logistic regression task on angel
  */

class LRRunner extends MLRunner {

  /**
    * Run LR train task
    * @param conf: configuration of algorithm and resource
    */
  override
  def train(conf: Configuration): Unit = {
    conf.setInt("angel.worker.matrixtransfer.request.timeout.ms", 60000)
    conf.set(AngelConfiguration.ANGEL_TASK_USER_TASKCLASS, classOf[LRTrainTask].getName)

    // Create an angel job client
    val client = AngelClientFactory.get(conf)

    // Submit this application
    client.submit()

    // Create a LR model
    val lrmodel = new LRModel(conf)

    // Model will be saved to HDFS, set the path
    lrmodel.setSavePath(conf)

    // Load model meta to client
    client.loadModel(lrmodel)

    // Start
    client.start()

    // Run user task and wait for completion,
    // User task is set in AngelConfiguration.ANGEL_TASK_USER_TASKCLASS
    client.waitForCompletion()

    // Save the trained model to HDFS
    client.saveModel(lrmodel)

    // Stop
    client.stop()
  }

  /*
   * Run LR predict task
   * @param conf: configuration of algorithm and resource
   */
  override
  def predict(conf: Configuration): Unit = {
    conf.setInt("angel.worker.matrixtransfer.request.timeout.ms", 60000)
    conf.set(AngelConfiguration.ANGEL_TASK_USER_TASKCLASS, classOf[LRPredictTask].getName)

    // Create an angel job client
    val client = AngelClientFactory.get(conf)

    // Submit this application
    client.submit()

    // Create LR model
    val lrmodel = new LRModel(conf)

    // A trained model will be loaded from HDFS to PS, set the HDFS path
    lrmodel.setLoadPath(conf)

    // Add the model meta to client
    client.loadModel(lrmodel)

    // Start
    client.start()

    // Run user task and wait for completion,
    // User task is set in AngelConfiguration.ANGEL_TASK_USER_TASKCLASS
    client.waitForCompletion()

    // Stop
    client.stop()
  }

  /*
   * Run LR incremental train task
   * @param conf: configuration of algorithm and resource
   */
  def incTrain(conf: Configuration): Unit = {
    conf.setInt("angel.worker.matrixtransfer.request.timeout.ms", 60000)
    conf.set(AngelConfiguration.ANGEL_TASK_USER_TASKCLASS, classOf[LRTrainTask].getName)

    // Create an angel job client
    val client = AngelClientFactory.get(conf)

    // Submit this application
    client.submit()

    // Create a LR model
    val lrmodel = new LRModel(conf)

    // The trained model will be loaded from HDFS to PS, set the HDFS path
    lrmodel.setLoadPath(conf)

    // The incremental learned model will be saved to HDFS, set the path
    lrmodel.setSavePath(conf)

    // Load model meta to client
    client.loadModel(lrmodel)

    // Start
    client.start()

    // Run user task and wait for completion,
    // User task is set in AngelConfiguration.ANGEL_TASK_USER_TASKCLASS
    client.waitForCompletion()

    // Save the incremental trained model to HDFS
    client.saveModel(lrmodel)

    // Stop
    client.stop()
  }
}

