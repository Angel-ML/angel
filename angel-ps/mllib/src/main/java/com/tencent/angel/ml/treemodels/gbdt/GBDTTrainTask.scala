package com.tencent.angel.ml.treemodels.gbdt

import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.task.TrainTask
import com.tencent.angel.ml.utils.DataParser
import com.tencent.angel.worker.storage.MemoryDataBlock
import com.tencent.angel.worker.task.TaskContext
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.{LongWritable, Text}

import scala.util.Random

class GBDTTrainTask (val ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx) {
  private val LOG = LogFactory.getLog(classOf[GBDTTrainTask])

  private val numFeature = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)

  private val validRatio = conf.getDouble(MLConf.ML_VALIDATE_RATIO, MLConf.DEFAULT_ML_VALIDATE_RATIO)
  private val validDataBlock = new MemoryDataBlock[LabeledData](-1)

  private val dataFormat = conf.get(MLConf.ML_DATA_FORMAT, "libsvm")
  private val dataParser = DataParser(dataFormat, numFeature, false)

  override def train(taskContext: TaskContext): Unit = {
    val trainer = new GBDTLearner(ctx)
    trainer.train(taskDataBlock, validDataBlock)
  }

  /**
    * Parse each sample into a labeled data, of which X is the feature weight vector, Y is label.
    */
  override def parse(key: LongWritable, value: Text): LabeledData = {
    dataParser.parse(value.toString)
  }

  /**
    * Separate train and validate data block
    */
  override def preProcess(taskContext: TaskContext): Unit = {
    val reader = taskContext.getReader
    while (reader.nextKeyValue()) {
      val out = parse(reader.getCurrentKey, reader.getCurrentValue)
      if (out != null) {
        if (Random.nextDouble() < validRatio) {
          validDataBlock.put(out)
        } else {
          taskDataBlock.put(out)
        }
      }
    }
    taskDataBlock.flush()
    validDataBlock.flush()
  }

}
