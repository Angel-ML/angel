package com.tencent.angel.ml.tree

import com.tencent.angel.ml.core.TrainTask
import com.tencent.angel.ml.core.conf.{MLConf, SharedConf}
import com.tencent.angel.ml.core.utils.DataParser
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.worker.storage.MemoryDataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.io.{LongWritable, Text}

class RandomForestTrainTask (val ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx) {

  private val validRatio = conf.getDouble(MLConf.ML_VALIDATE_RATIO, MLConf.DEFAULT_ML_VALIDATE_RATIO)

  // validation data storage
  var validDataBlock = new MemoryDataBlock[LabeledData](-1)
  val sharedConf: SharedConf = SharedConf.get(conf)
  override val dataParser = DataParser(sharedConf)

  /**
    * @param ctx : task context
    */
  @throws[Exception]
  def train(ctx: TaskContext) {
    val trainer = new RandomForestLearner(ctx)
    trainer.train(taskDataBlock, validDataBlock)
  }

  /**
    * parse the input text to trainning data
    *
    * @param key   the key
    * @param value the text
    */
  def parse(key: LongWritable, value: Text): LabeledData = {
    dataParser.parse(value.toString)
  }

  /**
    * before trainning, preprocess input text to trainning data and put them into trainning data
    * storage and validation data storage separately
    */
  override
  def preProcess(taskContext: TaskContext) {
    var count = 0
    val valid = Math.ceil(1.0 / validRatio).asInstanceOf[Int]

    val reader = taskContext.getReader

    while (reader.nextKeyValue) {
      val out = parse(reader.getCurrentKey, reader.getCurrentValue)
      if (out != null) {
        if (count % valid == 0)
          validDataBlock.put(out)
        else
          taskDataBlock.put(out)
        count += 1
      }
    }
    taskDataBlock.flush()
    validDataBlock.flush()
  }

}
