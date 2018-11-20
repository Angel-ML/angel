package com.tencent.angel.ml.tree

import com.tencent.angel.ml.core.PredictTask
import com.tencent.angel.ml.feature.LabeledData
import com.tencent.angel.worker.task.TaskContext
import org.apache.hadoop.io.{LongWritable, Text}
import com.tencent.angel.ml.tree.model.RandomForestModel

class RandomForestPredictTask (ctx: TaskContext) extends PredictTask[LongWritable, Text](ctx) {

  def predict(ctx: TaskContext) {
    predict(ctx, RandomForestModel(conf, ctx), taskDataBlock)
  }

  def parse(key: LongWritable, value: Text): LabeledData = {
    dataParser.parse(value.toString)
  }
}
