package com.tencent.angel.ml.modelparser

import com.tencent.angel.AppSubmitter
import com.tencent.angel.client.AngelClientFactory
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.conf.MLConf
import org.apache.hadoop.conf.Configuration


class ModelParserRunner extends AppSubmitter{
  /**
    * Submit application on Angel
    *
    * @param conf the conf
    */
  override def submit(conf: Configuration): Unit = {
    // For parse model task, we only start one task.
    // IF you want to do this paraller, set thread number via "ml.model.convert.thread.count"
    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1)
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1)
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, conf.get(MLConf.ML_MODEL_IN_PATH) + conf.get
    (MLConf.ML_MODEL_NAME));

    val client = AngelClientFactory.get(conf)
    client.startPSServer()
    //client.loadModel(model)
    client.runTask(classOf[ModelParserTask])
    client.waitForCompletion()
    //client.saveModel(model)

    client.stop()
  }
}
