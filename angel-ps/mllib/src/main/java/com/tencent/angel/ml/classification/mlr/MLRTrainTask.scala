package com.tencent.angel.ml.classification.mlr

import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.utils.DataParser
import com.tencent.angel.worker.storage.MemoryDataBlock
import com.tencent.angel.worker.task.{TaskContext, TrainTask}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.io.{LongWritable, Text}

/**
  * Created by hbghh on 2017/8/17.
  */
class MLRTrainTask(val ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx) {
  val LOG: Log = LogFactory.getLog(classOf[MLRTrainTask])

  // feature number of training data
  private val feaNum: Int = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
  // data format of training data, libsvm or dummy
  private val dataFormat = conf.get(MLConf.ML_DATAFORMAT, "dummy")
  // validate sample ratio
  private val valiRat = conf.getDouble(MLConf.ML_VALIDATE_RATIO, 0.05)

  // validation data storage
  var validDataBlock = new MemoryDataBlock[LabeledData](-1)

  override
  def train(ctx: TaskContext) {
    val trainer = new MLRLearner(ctx)
    trainer.train(trainDataBlock, validDataBlock)
  }

  /**
    * parse the input text to trainning data
    *
    * @param key   the key
    * @param value the text
    */
  override
  def parse(key: LongWritable, value: Text): LabeledData = {
    DataParser.parseVector(key, value, feaNum, dataFormat, negY = true)
  }

  /**
    * before trainning, preprocess input text to trainning data and put them into trainning data
    * storage and validation data storage separately
    */
  override
  def preProcess(taskContext: TaskContext) {
    val start = System.currentTimeMillis()

    var count = 0
    val vali = Math.ceil(1.0 / valiRat).asInstanceOf[Int]

    val reader = taskContext.getReader

    while (reader.nextKeyValue) {
      val out = parse(reader.getCurrentKey, reader.getCurrentValue)
      if (out != null) {
        if (count % vali == 0)
          validDataBlock.put(out)
        else
          trainDataBlock.put(out)
        count += 1
      }
    }
    trainDataBlock.flush()
    validDataBlock.flush()

    val cost = System.currentTimeMillis() - start
    LOG.info(s"Task[${ctx.getTaskIndex}] preprocessed ${trainDataBlock.size +
      validDataBlock.size} samples, ${trainDataBlock.size} for train, " +
      s"${validDataBlock.size} for validation. feanum=$feaNum")
  }

}
