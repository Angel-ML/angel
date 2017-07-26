package com.tencent.angel.ml.factorizationmachines

import com.tencent.angel.ml.conf.MLConf
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.math.vector.SparseDoubleSortedVector
import com.tencent.angel.worker.task.{TaskContext, TrainTask}
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.{LongWritable, Text}

class FMTrainTask (val ctx: TaskContext) extends TrainTask[LongWritable, Text](ctx) {
  val LOG = LogFactory.getLog(classOf[FMTrainTask])
  val feaNum = conf.getInt(MLConf.ML_FEATURE_NUM, MLConf.DEFAULT_ML_FEATURE_NUM)

  override def train(taskContext: TaskContext): Unit = {
    LOG.info("FM train task.")
    trainDataBlock.shuffle()

    val learner = new FMLearner(ctx)
    learner.train(trainDataBlock, null)
  }

  /**
    * Parse each sample into a labeled data, of which X is the feature weight vector, Y is label.
    */
  override
  def parse(key: LongWritable, line: Text): LabeledData = {
    if (null == line) {
      return null
    }
    val splits = line.toString().trim().split(" ")
    if (splits.length < 1) {
      return null
    }
    val nonzero = splits.length - 1
    val x = new SparseDoubleSortedVector(nonzero, feaNum)
    val y = splits(0).toDouble

    //TODO edit y according to task type
    //classification: 1 && -1.
    // regression: real value.
    for (i : Int <- 1 until splits.length ) {
      val tmp = splits(i)
      val sep = tmp.indexOf(":")
      if (sep != -1) {
        val idx = tmp.substring(0, sep).toInt
        val value = tmp.substring(sep + 1).toDouble
        x.set(idx - 1, value)
      }
    }
    new LabeledData(x, y)
  }
}
