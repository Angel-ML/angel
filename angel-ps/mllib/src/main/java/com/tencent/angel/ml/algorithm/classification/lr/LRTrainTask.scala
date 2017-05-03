package com.tencent.angel.ml.algorithm.classification.lr

import com.tencent.angel.ml.algorithm.conf.MLConf
import com.tencent.angel.ml.algorithm.utils.DataParser
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.worker.storage.MemoryStorage
import com.tencent.angel.worker.task.{TaskContext, TrainTask}
import org.apache.hadoop.io.{LongWritable, Text}


/**
  * Binomial logistic regression is a linear classification algorithm, each sample is labeled as
  * positive(+1) or negtive(-1). In this algorithm, the probability describing a single sample
  * being drawn from the positive class is modeled using a logistic function:
  * P(Y=+1|X) = 1 / [1+ exp(-dot(w,x))]. This task learns a binomial logistic regression model
  * with mini-batch gradient descent.
  *
  * @param ctx: task context
  **/


class LRTrainTask(val ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx) {
  // feature number of trainning data
  private val feaNum: Int = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)
  // data format of trainning data, libsvm or dummy
  private val dataFormat = conf.get(MLConf.ML_DATAFORMAT, "dummy")
  // validate sample ratio
  private val valiRat = conf.getDouble(MLConf.ML_VALIDATE_RATIO, 0.05)

  // validation data storage
  var validDataStorage = new MemoryStorage[LabeledData](-1)

  /**
    * @param ctx: task context
    */
  @throws[Exception]
  def run(ctx: TaskContext) {
    val trainer = new LRLeaner(ctx)
    trainer.train(trainDataStorage, validDataStorage)
  }

  /**
    * parse the input text to trainning data
    *
    * @param key   the key
    * @param value the text
    */
  override
  def parse(key: LongWritable, value: Text): LabeledData = {
    val sample = DataParser.parse(key, value, feaNum, "train", dataFormat)
    sample
  }

  /**
    * before trainning, preprocess input text to trainning data and put them into trainning data
    * storage and validation data storage separately
    */
  override
  def preProcess(taskContext: TaskContext) {
    var count = 0
    val vali = Math.ceil(1.0 / valiRat).asInstanceOf[Int]

    val reader = taskContext.getReader

    while (reader.nextKeyValue) {
      val out = parse(reader.getCurrentKey, reader.getCurrentValue)
      if (out != null) {
        if (count % vali == 0)
          validDataStorage.put(out)
        else
          trainDataStorage.put(out)
        count += 1
      }
    }

    trainDataStorage.flush()
    validDataStorage.flush()
  }

}