package com.tencent.angel.ml.treemodels.gbdt

import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.ml.task.PredictTask
import com.tencent.angel.ml.treemodels.gbdt.fp.FPGBDTModel
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.io.{LongWritable, Text}

class GBDTPredictTask (ctx: TaskContext) extends PredictTask[LongWritable, Text](ctx) {
  override def predict(taskContext: TaskContext): Unit = {
    predict(ctx, FPGBDTModel(conf, ctx), taskDataBlock)
  }

  /**
    * Parse each sample into a labeled data, of which X is the feature weight vector, Y is label.
    */
  override def parse(key: LongWritable, value: Text): LabeledData = {
    dataParser.parse(value.toString)
  }
}
