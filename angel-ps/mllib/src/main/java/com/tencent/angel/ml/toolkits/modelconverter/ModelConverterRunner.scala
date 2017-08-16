package com.tencent.angel.ml.toolkits.modelconverter

import com.tencent.angel.AppSubmitter
import com.tencent.angel.client.AngelClientFactory
import com.tencent.angel.conf.AngelConf
import com.tencent.angel.ml.conf.MLConf
import org.apache.hadoop.conf.Configuration

/**
  * Run model convert task on angel
  */
class ModelConverterRunner extends AppSubmitter{

  /**
    * Run model convert task
    *
    * @param conf : configuration of algorithm and resource
    */
  override def submit(conf: Configuration): Unit = {
    // For parse model task, we only start one task.
    // IF you want to do this in parallel, set thread number via "ml.model.convert.thread.count"
    conf.setInt(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1)
    conf.setInt(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1)
    conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, conf.get(MLConf.ML_MODEL_IN_PATH) + conf.get(MLConf.ML_MODEL_NAME));

    val client = AngelClientFactory.get(conf)
    client.startPSServer()
    client.runTask(classOf[ModelConverterTask])
    client.waitForCompletion()

    client.stop()
  }
}
