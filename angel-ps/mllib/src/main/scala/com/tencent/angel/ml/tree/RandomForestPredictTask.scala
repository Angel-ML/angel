package com.tencent.angel.ml.tree

import com.tencent.angel.ml.core.PredictTask
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.io.{LongWritable, Text}

class RandomForestPredictTask (ctx: TaskContext) extends PredictTask[LongWritable, Text](ctx) {

  def predict(ctx: TaskContext) {
    predict(ctx, RandomForestModel(ctx, conf), taskDataBlock);
  }

  def parse(key: LongWritable, value: Text): LabeledData = {
    dataParser.parse(value.toString)
  }
}
